package geph

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	defaultGephStartupTimeout = 15 * time.Second
	controlRPCPollInterval    = 100 * time.Millisecond
	controlRPCTimeout         = 500 * time.Millisecond
	gephStopTimeout           = 1 * time.Second
	maxControlRPCResponseSize = 64 * 1024
	maxStderrTailSize         = 8192
)

const gephReadinessRequestID = "sing-box-geph-readiness"
const gephStopRequestID = "sing-box-geph-stop"

type gephProcess struct {
	ctx                                context.Context
	executable, config, controlAddress string
	extraArgs                          []string
	timeout                            time.Duration
	incoming                           chan []byte
	outgoing                           chan []byte
	done, waitDone                     chan struct{}
	closeOnce                          sync.Once
	stateMu                            sync.Mutex
	closed                             bool
	ready                              bool
	cmd                                *exec.Cmd
	stdin                              io.WriteCloser
	waitErr                            error
	closeErr                           error
	stderrTail                         boundedStringBuffer
}

type gephControlRequest struct {
	JSONRPC string   `json:"jsonrpc"`
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      string   `json:"id"`
}

type gephControlResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      string          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type gephControlProtocolError struct {
	err error
}

func (e *gephControlProtocolError) Error() string {
	return e.err.Error()
}

func (e *gephControlProtocolError) Unwrap() error {
	return e.err
}

func newGephControlProtocolError(format string, args ...any) error {
	return &gephControlProtocolError{err: fmt.Errorf(format, args...)}
}

type boundedStringBuffer struct {
	mu      sync.Mutex
	content []byte
	limit   int
}

func newBoundedStringBuffer(limit int) boundedStringBuffer {
	return boundedStringBuffer{limit: limit}
}

func (b *boundedStringBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.limit > 0 && len(p) >= b.limit {
		b.content = append(b.content[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	if b.limit > 0 {
		excess := len(b.content) + len(p) - b.limit
		if excess > 0 {
			b.content = b.content[excess:]
		}
	}
	b.content = append(b.content, p...)
	return len(p), nil
}

func (b *boundedStringBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.content))
}

func newGephProcess(ctx context.Context, executable, config, controlAddress string, extraArgs []string, timeout time.Duration) *gephProcess {
	return &gephProcess{
		ctx:            ctx,
		executable:     executable,
		config:         config,
		controlAddress: controlAddress,
		extraArgs:      append([]string(nil), extraArgs...),
		timeout:        timeout,
		incoming:       make(chan []byte, 256),
		outgoing:       make(chan []byte, 256),
		done:           make(chan struct{}),
		waitDone:       make(chan struct{}),
		stderrTail:     newBoundedStringBuffer(maxStderrTailSize),
	}
}

func (p *gephProcess) args() []string {
	return append([]string{"--config", p.config, "--stdio-vpn"}, p.extraArgs...)
}

func (p *gephProcess) Start() error {
	if p.timeout <= 0 {
		p.timeout = defaultGephStartupTimeout
	}
	startupCtx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	p.stateMu.Lock()
	if p.closed {
		p.stateMu.Unlock()
		return fmt.Errorf("start Geph: process is closed")
	}
	if p.cmd != nil {
		p.stateMu.Unlock()
		return fmt.Errorf("start Geph: process is already started")
	}
	p.ready = false
	p.stateMu.Unlock()

	if err := p.ensureControlAddressAvailable(startupCtx); err != nil {
		if startupCtx.Err() != nil {
			return p.startupContextError(startupCtx, nil)
		}
		return err
	}

	cmd := exec.Command(p.executable, p.args()...)
	cmd.Stderr = &p.stderrTail
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("create Geph stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return fmt.Errorf("create Geph stdout: %w", err)
	}

	startResult := make(chan error, 1)
	go func() {
		startResult <- cmd.Start()
	}()

	select {
	case err = <-startResult:
	case <-startupCtx.Done():
		_ = stdin.Close()
		_ = stdout.Close()
		go reapLateGephStart(cmd, startResult)
		return p.startupContextError(startupCtx, nil)
	}
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return fmt.Errorf("start Geph: %w", err)
	}

	p.stateMu.Lock()
	p.cmd = cmd
	p.stdin = stdin
	p.stateMu.Unlock()

	go p.readLoop(stdout)
	go p.writeLoop(stdin)

	go func() {
		err := cmd.Wait()
		p.stateMu.Lock()
		p.waitErr = err
		p.closed = true
		p.ready = false
		p.stateMu.Unlock()
		close(p.waitDone)
		close(p.done)
	}()

	if err = p.waitUntilReady(startupCtx); err != nil {
		_ = p.Close()
		return err
	}
	p.stateMu.Lock()
	p.ready = true
	p.stateMu.Unlock()
	go func() {
		select {
		case <-p.ctx.Done():
			_ = p.Close()
		case <-p.waitDone:
		}
	}()
	return nil
}

func reapLateGephStart(cmd *exec.Cmd, startResult <-chan error) {
	if err := <-startResult; err == nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
}

func (p *gephProcess) ensureControlAddressAvailable(ctx context.Context) error {
	if p.controlAddress == "" {
		return nil
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", p.controlAddress)
	if err != nil {
		return fmt.Errorf("Geph control address %s is already in use or unavailable: %w", p.controlAddress, err)
	}
	if err := listener.Close(); err != nil {
		return fmt.Errorf("release Geph control address %s after availability check: %w", p.controlAddress, err)
	}
	return nil
}

func (p *gephProcess) waitUntilReady(startupCtx context.Context) error {
	if p.controlAddress == "" {
		return nil
	}

	ticker := time.NewTicker(controlRPCPollInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-startupCtx.Done():
			return p.startupContextError(startupCtx, lastErr)
		case <-p.waitDone:
			if p.ctx.Err() != nil {
				return p.startupContextError(startupCtx, lastErr)
			}
			return p.startupExitedError()
		default:
		}

		ready, state, err := p.queryConnectionState(startupCtx)
		if ready {
			select {
			case <-p.waitDone:
				if p.ctx.Err() != nil {
					return p.startupContextError(startupCtx, lastErr)
				}
				return p.startupExitedError()
			default:
				return nil
			}
		}
		if err != nil {
			var protocolErr *gephControlProtocolError
			if errors.As(err, &protocolErr) {
				return fmt.Errorf("invalid Geph control RPC response: %w", err)
			}
			lastErr = err
		} else if state != "" {
			switch state {
			case "Connecting", "Disconnected":
				lastErr = fmt.Errorf("control rpc state: %s", state)
			default:
				return fmt.Errorf("unexpected Geph control state: %s", state)
			}
		}

		select {
		case <-startupCtx.Done():
			return p.startupContextError(startupCtx, lastErr)
		case <-p.waitDone:
			if p.ctx.Err() != nil {
				return p.startupContextError(startupCtx, lastErr)
			}
			return p.startupExitedError()
		case <-ticker.C:
		}
	}
}

func (p *gephProcess) queryConnectionState(ctx context.Context) (bool, string, error) {
	var response struct {
		State string `json:"state"`
	}
	if err := p.queryControl(ctx, "conn_info", gephReadinessRequestID, []string{}, &response); err != nil {
		return false, "", err
	}
	state := strings.TrimSpace(response.State)
	switch state {
	case "Connected":
		return true, state, nil
	case "Connecting", "Disconnected":
		return false, state, nil
	default:
		return false, state, newGephControlProtocolError("conn_info rpc unexpected state %q", state)
	}
}

func (p *gephProcess) stop() error {
	if p.controlAddress == "" {
		return nil
	}
	select {
	case <-p.waitDone:
		return nil
	default:
	}
	if !p.isReady() {
		return nil
	}
	if err := p.queryControl(context.Background(), "stop", gephStopRequestID, []string{}, nil); err != nil {
		return err
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), gephStopTimeout)
	defer shutdownCancel()
	select {
	case <-p.waitDone:
		return nil
	case <-shutdownCtx.Done():
		return fmt.Errorf("timeout waiting for geph stop")
	}
}

func (p *gephProcess) isReady() bool {
	p.stateMu.Lock()
	ready := p.ready
	p.stateMu.Unlock()
	return ready
}

func (p *gephProcess) queryControl(ctx context.Context, method, requestID string, params []string, result any) error {
	attemptCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(attemptCtx, "tcp", p.controlAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	if deadline, ok := attemptCtx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("set %s deadline: %w", method, err)
		}
	}

	request := gephControlRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      requestID,
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", method, err)
	}
	requestBytes = append(requestBytes, '\n')
	if err := writeFull(conn, requestBytes); err != nil {
		return fmt.Errorf("send %s request: %w", method, err)
	}

	reader := bufio.NewReader(io.LimitReader(conn, maxControlRPCResponseSize+1))
	responseLine, err := reader.ReadString('\n')
	if len(responseLine) > maxControlRPCResponseSize {
		return newGephControlProtocolError("%s response exceeds %d bytes", method, maxControlRPCResponseSize)
	}
	if err != nil {
		return fmt.Errorf("read %s response: %w", method, err)
	}
	if strings.TrimSpace(responseLine) == "" {
		return newGephControlProtocolError("%s response line empty", method)
	}
	var response gephControlResponse
	if err := json.Unmarshal([]byte(responseLine), &response); err != nil {
		return newGephControlProtocolError("parse %s response: %w", method, err)
	}
	if response.JSONRPC != "2.0" {
		return newGephControlProtocolError("%s rpc returned JSON-RPC version %q", method, response.JSONRPC)
	}
	if response.ID != requestID {
		return newGephControlProtocolError("%s rpc returned mismatched id %q", method, response.ID)
	}
	if response.Error != nil {
		if response.Error.Message != "" {
			return newGephControlProtocolError("%s rpc error: %s (code=%d)", method, response.Error.Message, response.Error.Code)
		}
		return newGephControlProtocolError("%s rpc error: code=%d", method, response.Error.Code)
	}
	if len(response.Result) == 0 {
		return newGephControlProtocolError("%s rpc missing result", method)
	}
	if result != nil {
		if err := json.Unmarshal(response.Result, result); err != nil {
			return newGephControlProtocolError("parse %s response result: %w", method, err)
		}
	}
	return nil
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func (p *gephProcess) startupContextError(startupCtx context.Context, lastErr error) error {
	if err := p.ctx.Err(); err != nil {
		if lastErr != nil {
			return fmt.Errorf("start Geph: %w (last control RPC error: %v)", err, lastErr)
		}
		return fmt.Errorf("start Geph: %w", err)
	}
	if errors.Is(startupCtx.Err(), context.DeadlineExceeded) {
		return p.startupTimeoutError(lastErr)
	}
	if err := startupCtx.Err(); err != nil {
		return fmt.Errorf("start Geph: %w", err)
	}
	return fmt.Errorf("start Geph: startup canceled")
}

func (p *gephProcess) startupTimeoutError(lastErr error) error {
	reason := strings.TrimSpace(p.stderrTail.String())
	if reason == "" {
		if lastErr != nil {
			return fmt.Errorf("start Geph: timeout waiting for control RPC after %s: %w", p.timeout, lastErr)
		}
		return fmt.Errorf("start Geph: timeout waiting for control RPC after %s", p.timeout)
	}
	if lastErr != nil {
		return fmt.Errorf("start Geph: timeout waiting for control RPC after %s: %w (%s)", p.timeout, lastErr, reason)
	}
	return fmt.Errorf("start Geph: timeout waiting for control RPC after %s (%s)", p.timeout, reason)
}

func (p *gephProcess) startupExitedError() error {
	p.stateMu.Lock()
	waitErr := p.waitErr
	p.stateMu.Unlock()

	reason := strings.TrimSpace(p.stderrTail.String())
	if waitErr == nil {
		if reason == "" {
			return fmt.Errorf("start Geph: process exited during startup")
		}
		return fmt.Errorf("start Geph: process exited during startup: %s", reason)
	}
	if reason == "" {
		return fmt.Errorf("start Geph: process exited during startup: %w", waitErr)
	}
	return fmt.Errorf("start Geph: process exited during startup: %w (%s)", waitErr, reason)
}

func (p *gephProcess) sendPacket(packet []byte) error {
	if len(packet) == 0 || len(packet) > 65535 {
		return fmt.Errorf("invalid Geph packet length: %d", len(packet))
	}
	p.stateMu.Lock()
	closed := p.closed
	p.stateMu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	copyPacket := append([]byte(nil), packet...)
	select {
	case <-p.done:
		return io.ErrClosedPipe
	default:
	}
	select {
	case p.outgoing <- copyPacket:
		return nil
	case <-p.done:
		return io.ErrClosedPipe
	}
}

func (p *gephProcess) readLoop(r io.Reader) {
	defer close(p.incoming)
	var length [2]byte
	for {
		if _, err := io.ReadFull(r, length[:]); err != nil {
			return
		}
		n := int(binary.BigEndian.Uint16(length[:]))
		if n == 0 {
			continue
		}
		packet := make([]byte, n)
		if _, err := io.ReadFull(r, packet); err != nil {
			return
		}
		select {
		case p.incoming <- packet:
		case <-p.done:
			return
		}
	}
}

func (p *gephProcess) writeLoop(stdin io.Writer) {
	for {
		var packet []byte
		select {
		case packet = <-p.outgoing:
		case <-p.done:
			return
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(packet)))
		if _, err := stdin.Write(length[:]); err != nil {
			return
		}
		if _, err := stdin.Write(packet); err != nil {
			return
		}
	}
}

func (p *gephProcess) Close() error {
	p.closeOnce.Do(func() {
		p.stateMu.Lock()
		closedCmd := p.cmd
		closedStdin := p.stdin
		p.cmd = nil
		p.stdin = nil
		p.closed = true
		p.stateMu.Unlock()
		if closedCmd != nil && closedCmd.Process != nil {
			stopErr := p.stop()
			if closedStdin != nil {
				_ = closedStdin.Close()
			}
			select {
			case <-p.waitDone:
				return
			default:
			}
			killErr := closedCmd.Process.Kill()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), gephStopTimeout)
			defer shutdownCancel()
			select {
			case <-p.waitDone:
				return
			case <-shutdownCtx.Done():
				if stopErr != nil {
					stopErr = fmt.Errorf("gracefully stop Geph: %w", stopErr)
				}
				if killErr != nil {
					killErr = fmt.Errorf("kill Geph: %w", killErr)
				}
				p.closeErr = errors.Join(stopErr, killErr, fmt.Errorf("wait for Geph exit after kill: %w", shutdownCtx.Err()))
			}
		} else if closedStdin != nil {
			_ = closedStdin.Close()
		}
	})
	return p.closeErr
}
