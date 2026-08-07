# 引言

sing-box 使用 JSON 作为配置文件格式。
### 结构

```json
{
  "$schema": "https://sing-box.sagernet.org/schema.json",
  "log": {},
  "dns": {},
  "ntp": {},
  "certificate": {},
  "certificate_providers": [],
  "http_clients": [],
  "network_namespaces": [],
  "endpoints": [],
  "inbounds": [],
  "outbounds": [],
  "route": {},
  "services": [],
  "experimental": {}
}
```

### 字段

| Key            | Format                 |
|----------------|------------------------|
| `$schema`      | [JSON Schema](./schema/) |
| `log`          | [日志](./log/)           |
| `dns`          | [DNS](./dns/)          |
| `ntp`          | [NTP](./ntp/)          |
| `certificate`  | [证书](./certificate/)   |
| `certificate_providers` | [证书提供者](./shared/certificate-provider/) |
| `http_clients` | [HTTP 客户端](./shared/http-client/) |
| `network_namespaces` | [网络命名空间](./network-namespace/) |
| `endpoints`    | [端点](./endpoint/)      |
| `inbounds`     | [入站](./inbound/)       |
| `outbounds`    | [出站](./outbound/)      |
| `route`        | [路由](./route/)         |
| `services`     | [服务](./service/)       |
| `experimental` | [实验性](./experimental/) |

### 检查

```bash
sing-box check
```

如需校验 Geph 端点就绪与控制端口占用情况，可执行：

```bash
sing-box check --start-geph
```

该选项会启动配置文件中所有 Geph 端点、等待每个端点上报 `Connected`，然后停止它们。

### 格式化

```bash
sing-box format -w -c config.json -D config_directory
```

### 合并

```bash
sing-box merge output.json -c config.json -D config_directory
```
