<img width="1920" height="953" alt="image" src="https://github.com/user-attachments/assets/b0f53714-fe44-442d-997d-4b03f9c52116" />
# ODA ILOM Inspector

**Oracle Database Appliance X11 HA 硬件巡检工具**

通过 SSH 连接 Oracle ILOM 5.x，采集硬件健康数据，生成 HTML 报告，并可选推送企业微信。

---

## 功能

- **硬件状态采集**：CPU、内存（DIMM 槽位）、电源模块、冷却/风扇、磁盘、PCI 设备
- **指示灯 & 系统健康**：电源状态、定位灯、FAULT 状态（从 `/System` 属性读取）
- **事件日志**：最近 50 条 ILOM 事件（支持 ILOM 5.x `Weekday Mon DD HH:MM:SS YYYY` 格式）
- **历史趋势图**：近 7 天入口温度、排风温度、功耗、风扇转速折线图（Chart.js）
- **企业微信推送**：Markdown 摘要 + HTML 报告文件
- **多节点并列**：一份报告展示所有节点，同组对比一目了然

## 适用设备

| 项目 | 值 |
|------|----|
| 硬件 | Oracle Database Appliance X11 HA（双节点或四节点） |
| ILOM 版本 | 5.x（已在 ILOM 5.1.4.23 实测） |
| 账号权限 | 只读账号（`monitor` 角色）即可 |

> **注意**：ILOM 4.x 字段名与路径不同，未经测试。

## 安装

```bash
pip install paramiko requests pyyaml
```

Python 3.8 或以上，Windows / Linux 均可运行。

## 快速开始

```bash
# 1. 克隆仓库
git clone https://github.com/Gunther8/oda-ilom-inspector.git
cd oda-ilom-inspector

# 2. 安装依赖
pip install paramiko requests pyyaml

# 3. 创建配置文件
cp config.example.yaml config.yaml

# 4. 编辑 config.yaml，填写节点 IP 和密码
#    （可选）填写企业微信 Webhook key

# 5. 运行
python oda_check.py

# 仅生成报告，不推送企业微信
python oda_check.py --no-send
```

生成的 HTML 报告保存在脚本同目录下，文件名格式 `oda_report_YYYYMMDD_HHmmss.html`。

## 配置说明

```yaml
nodes:
  - name: "生产-节点0"       # 显示名称（自定义）
    host: "192.168.1.100"   # ILOM 管理口 IP
    username: "monitor"     # ILOM 只读账号
    password: "your_pwd"    # 密码

wecom:
  webhook_key: ""           # 企业微信机器人 key，留空则不推送

advanced:
  ssh_connect_timeout: 30
  cmd_timeout: 60
  report_retain_days: 30    # HTML 报告保留天数，超期自动删除
```

## 定时运行（Windows 任务计划程序）

以管理员身份打开 PowerShell，执行：

```powershell
$action = New-ScheduledTaskAction `
    -Execute "python" `
    -Argument "C:\path\to\oda_check.py" `
    -WorkingDirectory "C:\path\to\"

$trigger = New-ScheduledTaskTrigger -Daily -At "07:38"

Register-ScheduledTask `
    -TaskName "ODA硬件巡检" `
    -Action $action `
    -Trigger $trigger `
    -RunLevel Highest `
    -Force
```

## ILOM 已知限制（ILOM 5.x）

| 路径 | 状态 | 说明 |
|------|------|------|
| `/System` | ✅ | health、power_state、locator_indicator 等 |
| `/System/Processors/CPUs/CPU_*` | ✅ | CPU 健康、型号、核数（温度标注 Not Supported）|
| `/System/Memory/DIMMs/DIMM_*` | ✅ | DIMM 槽位详情 |
| `/System/Power/Power_Supplies/*` | ✅ | PSU 健康、输入/输出功率 |
| `/System/Cooling` | ✅ | 入口温度、排风温度、风扇数 |
| `/SP/logs/event/list` | ✅ | 事件日志（自动翻页）|
| `/SP/Sensors/*` | ❌ | Invalid target（ILOM 5.x 不支持）|
| `/System/Indicators` | ❌ | Invalid target |
| `/System/Cooling/Fans/*` | ❌ | monitor 账号无详细风扇数据 |

## 报告截图

> *(运行后将 oda_report_*.html 用浏览器打开即可查看)*

## License

MIT
