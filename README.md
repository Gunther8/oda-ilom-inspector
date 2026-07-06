<img width="1920" height="953" alt="image" src="https://github.com/user-attachments/assets/b0f53714-fe44-442d-997d-4b03f9c52116" />
# ODA ILOM Inspector

**Oracle Database Appliance X11 HA 硬件巡检工具 · Hardware Inspection Tool**

通过 SSH 连接 Oracle ILOM 5.x，采集硬件健康数据，生成 HTML 报告，并可选推送企业微信。

Connects to Oracle ILOM 5.x via SSH, collects hardware health data, generates an HTML report, and optionally pushes a summary to WeCom (企业微信).

---

## 功能 · Features

- **硬件状态采集** · Hardware collection: CPU、内存（DIMM 槽位）、电源模块、冷却/风扇、磁盘、PCI 设备
- **指示灯 & 系统健康** · LED & system health: 电源状态、定位灯、FAULT 状态（从 `/System` 属性读取）
- **事件日志** · Event log: 最近 50 条 ILOM 事件，支持 ILOM 5.x `Weekday Mon DD HH:MM:SS YYYY` 格式
- **历史趋势图** · Historical trends: 近 7 天入口/排风温度、功耗、风扇转速折线图（Chart.js）
- **企业微信推送** · WeCom push: Markdown 摘要 + HTML 报告文件
- **多节点并列** · Multi-node: 一份报告展示所有节点，同组对比一目了然

## 适用设备 · Compatibility

| 项目 | 值 |
|------|----|
| 硬件 Hardware | Oracle Database Appliance X11 HA（双节点或四节点） |
| ILOM 版本 ILOM version | 5.x（已在 ILOM 5.1.4.23 实测 / tested on ILOM 5.1.4.23） |
| 账号权限 Account | 只读账号（`monitor` 角色）即可 / read-only `monitor` role |

> **注意 Note**：ILOM 4.x 字段名与路径不同，未经测试。Field names and paths differ on ILOM 4.x — not tested.

## 安装 · Installation

```bash
pip install paramiko requests pyyaml
```

Python 3.8+，Windows / Linux 均可运行。  
Requires Python 3.8 or later. Works on Windows and Linux.

## 快速开始 · Quick Start

```bash
# 1. Clone the repo / 克隆仓库
git clone https://github.com/Gunther8/oda-ilom-inspector.git
cd oda-ilom-inspector

# 2. Install dependencies / 安装依赖
pip install paramiko requests pyyaml

# 3. Create config / 创建配置文件
cp config.example.yaml config.yaml   # Linux/macOS
# copy config.example.yaml config.yaml   # Windows

# 4. Edit config.yaml — fill in node IPs and passwords
#    编辑 config.yaml，填写节点 IP 和密码
#    (Optional) fill in WeCom webhook key / （可选）填写企业微信 Webhook key

# 5. Run / 运行
python oda_check.py

# Generate report only, skip WeCom push / 仅生成报告，不推送企业微信
python oda_check.py --no-send
```

The HTML report is saved in the same directory as the script, named `oda_report_YYYYMMDD_HHmmss.html`.  
HTML 报告保存在脚本同目录下，文件名格式 `oda_report_YYYYMMDD_HHmmss.html`。

## 配置说明 · Configuration

```yaml
nodes:
  - name: "Production-Node0"    # Display name / 显示名称（自定义）
    host: "192.168.1.100"       # ILOM management IP / ILOM 管理口 IP
    username: "monitor"         # ILOM read-only account / ILOM 只读账号
    password: "your_pwd"

wecom:
  webhook_key: ""               # WeCom robot key; leave empty to disable push
                                # 企业微信机器人 key，留空则不推送

advanced:
  ssh_connect_timeout: 30       # SSH connection timeout (seconds)
  cmd_timeout: 60               # Per-command timeout (seconds)
  report_retain_days: 30        # Days to keep local HTML reports / HTML 报告保留天数
```

## 定时运行 · Scheduled Execution

### Windows 任务计划程序 · Windows Task Scheduler

以管理员身份打开 PowerShell / Open PowerShell as Administrator:

```powershell
$action = New-ScheduledTaskAction `
    -Execute "python" `
    -Argument "C:\path\to\oda_check.py" `
    -WorkingDirectory "C:\path\to\"

$trigger = New-ScheduledTaskTrigger -Daily -At "07:38"

Register-ScheduledTask `
    -TaskName "ODA-ILOM-Inspector" `
    -Action $action `
    -Trigger $trigger `
    -RunLevel Highest `
    -Force
```

### Linux crontab

```bash
38 7 * * * cd /path/to/oda-ilom-inspector && python oda_check.py >> oda_report.log 2>&1
```

## ILOM 已知限制 · Known ILOM 5.x Path Limitations

| Path | Status | Notes |
|------|--------|-------|
| `/System` | ✅ | health, power_state, locator_indicator, etc. |
| `/System/Processors/CPUs/CPU_*` | ✅ | CPU health, model, core count (temperature = Not Supported) |
| `/System/Memory/DIMMs/DIMM_*` | ✅ | Per-slot DIMM details |
| `/System/Power/Power_Supplies/*` | ✅ | PSU health, input/output power |
| `/System/Cooling` | ✅ | Inlet temp, exhaust temp, fan count |
| `/SP/logs/event/list` | ✅ | Event log (auto-pagination) |
| `/SP/Sensors/*` | ❌ | Invalid target on ILOM 5.x |
| `/System/Indicators` | ❌ | Invalid target |
| `/System/Cooling/Fans/*` | ❌ | No data with read-only account |

## 报告截图 · Report Preview

> 运行后将 `oda_report_*.html` 用浏览器打开即可查看。  
> Open any generated `oda_report_*.html` in a browser to preview the report.

## License

MIT
