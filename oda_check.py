#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Oracle ODA X11 HA 硬件巡检脚本
适用设备: Oracle Database Appliance X11 High Availability (ILOM 5.x)
运行环境: Python 3.8+  (Windows / Linux)
依赖:     pip install paramiko requests pyyaml

配置方式: 复制 config.example.yaml 为 config.yaml，填写节点 IP 和密码。
"""

# ============================================================
# 配置加载 —— 从 config.yaml 读取，缺失时报错提示
# ============================================================
import sys, os
from pathlib import Path

_SCRIPT_DIR = Path(__file__).parent.resolve()
_CFG_FILE   = _SCRIPT_DIR / "config.yaml"

if not _CFG_FILE.exists():
    print(f"[ERROR] 缺少配置文件: {_CFG_FILE}")
    print("        请复制 config.example.yaml 为 config.yaml 并填写节点信息。")
    sys.exit(1)

try:
    import yaml
except ImportError:
    print("[ERROR] 缺少 pyyaml，请运行: pip install pyyaml")
    sys.exit(1)

with open(_CFG_FILE, encoding="utf-8") as _f:
    _cfg = yaml.safe_load(_f)

NODES = _cfg["nodes"]

_wecom            = _cfg.get("wecom", {})
WECOM_KEY         = _wecom.get("webhook_key", "")
SEND_URL          = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={WECOM_KEY}"
UPLOAD_URL        = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={WECOM_KEY}&type=file"

_adv                 = _cfg.get("advanced", {})
SSH_CONNECT_TIMEOUT  = _adv.get("ssh_connect_timeout", 30)
CMD_TIMEOUT          = _adv.get("cmd_timeout", 60)
ILOM_PROMPT          = "->"
WECOM_RETRY_COUNT    = _adv.get("wecom_retry_count", 3)
WECOM_RETRY_INTERVAL = _adv.get("wecom_retry_interval", 5)
REPORT_RETAIN_DAYS   = _adv.get("report_retain_days", 30)

# ============================================================
# 标准库
# ============================================================
import re, glob, json, time, logging, sqlite3
import datetime, traceback
from typing import Dict, List, Optional, Tuple

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="paramiko")
import paramiko
import requests

# ============================================================
# 路径 & 日志
# ============================================================
SCRIPT_DIR = Path(__file__).parent.resolve()
DB_FILE    = SCRIPT_DIR / "oda_history.db"
LOG_FILE   = SCRIPT_DIR / "oda_report.log"

logging.basicConfig(
    filename=str(LOG_FILE), level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S", encoding="utf-8",
)
logger = logging.getLogger(__name__)
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_ch)


# ============================================================
# SQLite
# ============================================================
def init_db() -> None:
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, node_name TEXT NOT NULL,
            inlet_temp REAL, exhaust_temp REAL,
            actual_power_consumption REAL, fan_percentage_avg REAL,
            system_health TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, node_name)
        )""")
    conn.commit(); conn.close()

def save_metrics(date_str: str, node_name: str, m: dict) -> None:
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("""
        INSERT INTO daily_metrics
            (date, node_name, inlet_temp, exhaust_temp,
             actual_power_consumption, fan_percentage_avg, system_health)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(date, node_name) DO UPDATE SET
            inlet_temp=excluded.inlet_temp,
            exhaust_temp=excluded.exhaust_temp,
            actual_power_consumption=excluded.actual_power_consumption,
            fan_percentage_avg=excluded.fan_percentage_avg,
            system_health=excluded.system_health
    """, (date_str, node_name, m.get("inlet_temp"), m.get("exhaust_temp"),
          m.get("actual_power_consumption"), m.get("fan_percentage_avg"),
          m.get("system_health")))
    conn.commit(); conn.close()

def get_history(node_name: str, days: int = 7) -> List[dict]:
    conn = sqlite3.connect(str(DB_FILE))
    rows = conn.execute("""
        SELECT date, inlet_temp, exhaust_temp, actual_power_consumption,
               fan_percentage_avg, system_health
        FROM daily_metrics WHERE node_name=? ORDER BY date DESC LIMIT ?
    """, (node_name, days)).fetchall()
    conn.close()
    return list(reversed([
        {"date": r[0], "inlet_temp": r[1], "exhaust_temp": r[2],
         "actual_power_consumption": r[3], "fan_percentage_avg": r[4],
         "system_health": r[5]}
        for r in rows
    ]))

def get_yesterday(node_name: str) -> Optional[dict]:
    yd = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(str(DB_FILE))
    row = conn.execute("""
        SELECT inlet_temp, exhaust_temp, actual_power_consumption,
               fan_percentage_avg, system_health
        FROM daily_metrics WHERE node_name=? AND date=?
    """, (node_name, yd)).fetchone()
    conn.close()
    if row:
        return {"inlet_temp": row[0], "exhaust_temp": row[1],
                "actual_power_consumption": row[2], "fan_percentage_avg": row[3],
                "system_health": row[4]}
    return None


# ============================================================
# SSH / ILOM 客户端
# ============================================================
class ILOMClient:
    def __init__(self, host, username, password):
        self.host = host; self.username = username; self.password = password
        self._client = None; self._channel = None

    def connect(self):
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(self.host, username=self.username, password=self.password,
                             timeout=SSH_CONNECT_TIMEOUT, look_for_keys=False, allow_agent=False)
        self._channel = self._client.invoke_shell(width=240, height=50)
        self._channel.settimeout(CMD_TIMEOUT)
        self._drain()
        logger.info("[%s] ILOM 连接成功", self.host)

    def _drain(self) -> str:
        buf = ""; deadline = time.time() + CMD_TIMEOUT
        while time.time() < deadline:
            if self._channel.recv_ready():
                chunk = self._channel.recv(8192).decode("utf-8", errors="replace")
                buf += chunk
                # 遇到 ILOM 分页提示 → 自动发空格继续，并从缓冲中擦除该提示行
                if re.search(r"press any key to continue", buf, re.IGNORECASE):
                    self._channel.send(" ")
                    buf = re.sub(r"[^\n]*press any key to continue[^\n]*", "", buf, flags=re.IGNORECASE)
                    continue
                if ILOM_PROMPT in buf:
                    return buf
            else:
                time.sleep(0.15)
        return buf

    def run(self, cmd: str) -> str:
        self._channel.send(cmd + "\n")
        raw = self._drain()
        lines = []
        for line in raw.splitlines():
            s = line.strip()
            if s == cmd.strip(): continue
            if s == ILOM_PROMPT or re.match(r".*->\s*$", s): continue
            lines.append(line)
        return "\n".join(lines)

    def disconnect(self):
        try:
            if self._channel: self._channel.close()
            if self._client:  self._client.close()
        except Exception: pass
        logger.info("[%s] 连接已关闭", self.host)


# ============================================================
# 采集命令函数（每条命令独立封装）
# ============================================================
# ---- 原有命令 ----
def cmd_system(c):               return c.run("show /System")
def cmd_cpu(c, cid):             return c.run(f"show /System/Processors/CPUs/{cid}")
def cmd_memory(c):               return c.run("show /System/Memory")
def cmd_dimm_list(c):            return c.run("show /System/Memory/DIMMs")
def cmd_dimm(c, did):            return c.run(f"show /System/Memory/DIMMs/{did}")
def cmd_power(c):                return c.run("show /System/Power")
def cmd_psu(c, pid):             return c.run(f"show /System/Power/Power_Supplies/{pid}")
def cmd_cooling(c):              return c.run("show /System/Cooling")
def cmd_fan(c, fid):             return c.run(f"show /System/Cooling/Fans/{fid}")
def cmd_storage(c):              return c.run("show /System/Storage")
def cmd_disk_list(c):            return c.run("show /System/Storage/Disks")
def cmd_disk(c, did):            return c.run(f"show /System/Storage/Disks/{did}")
def cmd_networking(c):           return c.run("show /System/Networking")
def cmd_nic(c, nid="Ethernet_NIC_0"): return c.run(f"show /System/Networking/Ethernet_NICs/{nid}")
def cmd_pci_list(c):             return c.run("show /System/PCI_Devices/Add-on")
def cmd_pci_dev(c, did):         return c.run(f"show /System/PCI_Devices/Add-on/{did}")
def cmd_event_log(c):            return c.run("show /SP/Logs/event/list")

# ---- 新增命令（monitor 账号可访问）----
def cmd_open_problems(c):        return c.run("show /System/Open_Problems")
def cmd_open_problem(c, pid):    return c.run(f"show /System/Open_Problems/{pid}")
def cmd_sp(c):                   return c.run("show /SP")
def cmd_sp_network(c):           return c.run("show /SP/Network")
def cmd_sp_clock(c):             return c.run("show /SP/Clock")
def cmd_bios(c):                 return c.run("show /System/BIOS")
def cmd_board_mb(c):             return c.run("show /System/Boards/MB")
def cmd_indicators(c):           return c.run("show /System/Indicators")
# ODA x86 传感器路径在 /SP/Sensors 而非 /System/Sensors
def cmd_sp_sensors_temp_list(c): return c.run("show /SP/Sensors/Temperature")
def cmd_sp_sensor_temp(c, sid):  return c.run(f"show /SP/Sensors/Temperature/{sid}")
def cmd_sp_sensors_volt_list(c): return c.run("show /SP/Sensors/Voltage")
def cmd_sp_sensor_volt(c, sid):  return c.run(f"show /SP/Sensors/Voltage/{sid}")
def cmd_sp_sensors_fan_list(c):  return c.run("show /SP/Sensors/Fan")
def cmd_sp_sensor_fan(c, sid):   return c.run(f"show /SP/Sensors/Fan/{sid}")
def cmd_firmware(c):             return c.run("show /System/Firmware")
def cmd_fault_mgmt(c):           return c.run("show /System/FaultMgmt")


# ============================================================
# 解析函数
# ============================================================
def parse_props(text: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    for line in text.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            key = k.strip().lower().replace(" ", "_").replace("-", "_")
            props[key] = v.strip()
    return props

def parse_targets(text: str) -> List[str]:
    items: List[str] = []; in_t = False
    for line in text.splitlines():
        s = line.strip()
        if re.match(r"Targets\s*:", s, re.IGNORECASE):
            in_t = True; continue
        if in_t:
            if not s or re.match(r"(Properties|Commands)\s*:", s, re.IGNORECASE): break
            items.append(s)
    return items

def parse_number(value: str) -> Optional[float]:
    m = re.search(r"(\d+(?:\.\d+)?)", value or "")
    return float(m.group(1)) if m else None

def parse_events(text: str) -> List[dict]:
    """解析 ILOM 5.x 事件日志两种格式：
    格式A (ILOM 5.x): ID  Weekday Mon DD HH:MM:SS YYYY  Class  Type  Severity
                       (下一缩进行) Description
    格式B (旧):        ID  YYYY-MM-DD HH:MM:SS  Class  Type  Severity  Description
    """
    events: List[dict] = []
    # 去掉空行和 ILOM 分页提示行，保留原始缩进以便识别描述行
    raw_lines = [l for l in text.splitlines()
                 if l.strip() and not re.search(r"press any key to continue", l, re.IGNORECASE)]
    header_found = False

    # 正则：格式A — "42     Fri Oct 31 10:35:10 2025  Sensor    Log       minor"
    _re_a = re.compile(
        r"^(\d+)\s+"
        r"(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+"
        r"(\S+)\s+(\S+)\s+"
        r"(major|minor|info|critical|normal)\s*$",
        re.IGNORECASE,
    )
    # 正则：格式B — "42     2025-10-31 10:35:10  Sensor  Log  minor  Desc"
    _re_b = re.compile(
        r"^(\d+)\s+"
        r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
        r"(\S+)\s+(\S+)\s+"
        r"(major|minor|info|critical|normal)\s*(.*)",
        re.IGNORECASE,
    )

    i = 0
    while i < len(raw_lines):
        s = raw_lines[i].strip()
        if not header_found:
            if re.search(r"\bID\b.*\bDate", s, re.IGNORECASE):
                header_found = True
            i += 1; continue

        m = _re_a.match(s)
        if m:
            desc = ""
            # 下一行若非事件行且有缩进则是描述
            if i + 1 < len(raw_lines):
                nxt = raw_lines[i + 1]
                if not _re_a.match(nxt.strip()) and not _re_b.match(nxt.strip()):
                    desc = nxt.strip()
                    i += 1
            events.append({"id": m.group(1), "datetime": m.group(2),
                           "class": m.group(3), "type": m.group(4),
                           "severity": m.group(5).lower(), "description": desc})
            i += 1; continue

        m = _re_b.match(s)
        if m:
            events.append({"id": m.group(1), "datetime": m.group(2),
                           "class": m.group(3), "type": m.group(4),
                           "severity": m.group(5).lower(),
                           "description": m.group(6).strip()})
        i += 1

    return events[-50:]

def _prop(d: dict, *keys, default: str = "N/A") -> str:
    """多候选键名的宽松查找，首个非空值为准。"""
    for k in keys:
        v = d.get(k, "")
        if v and v.strip() not in ("", "None", "none"):
            return v
    # 部分匹配兜底
    for k in keys:
        for dk, dv in d.items():
            if k in dk and dv and dv.strip() not in ("", "None", "none"):
                return dv
    return default

def _fan_pct(d: dict) -> Optional[float]:
    """兼容多种 ILOM 风扇速度字段，返回百分比数值。"""
    for key in ("fan_percentage", "target_fan_speed", "speed_percentage",
                "fan_speed_percentage", "actual_speed"):
        v = d.get(key, "")
        if v:
            # 若含 RPM 则跳过（不是百分比）
            if "rpm" in v.lower(): continue
            n = parse_number(v)
            if n is not None and n <= 100:
                return n
    return None

def _fan_rpm(d: dict) -> Optional[float]:
    """尝试读取 RPM 速度字段。"""
    for key in ("speed", "actual_speed", "fan_speed", "rotational_speed"):
        v = d.get(key, "")
        if v and "rpm" in v.lower():
            return parse_number(v)
    return None


# ============================================================
# 节点完整数据采集
# ============================================================
def collect_node(node_cfg: dict) -> dict:
    host = node_cfg["host"]; nm = node_cfg["name"]
    data = {
        "node_name": nm, "host": host, "error": None,
        "system": {}, "cpus": {},
        "memory":     {"summary": {}, "dimms": []},
        "power":      {"summary": {}, "psus": []},
        "cooling":    {"summary": {}, "fans": []},
        "storage":    {"summary": {}, "disks": []},
        "networking": {"summary": {}, "nics": []},
        "pci_devices": [],
        "events": [],
        # 新增
        "open_problems": [],
        "sp": {}, "sp_network": {}, "sp_clock": {},
        "bios": {}, "board_mb": {},
        "indicators": {},
        "sensors_temp": [],
        "sensors_volt": [],
        "firmware": {},
        "fault_mgmt": {},
    }
    client = ILOMClient(host, node_cfg["username"], node_cfg["password"])
    try:
        client.connect()

        # ---- 系统概览 ----
        data["system"] = parse_props(cmd_system(client))
        logger.info("[%s] 系统概览 OK", host)

        # ---- 处理器 ----
        for cid in ("CPU_0", "CPU_1"):
            try:
                data["cpus"][cid] = parse_props(cmd_cpu(client, cid))
            except Exception as e:
                logger.warning("[%s] %s 失败: %s", host, cid, e)
                data["cpus"][cid] = {"_error": str(e)}

        # ---- 内存（先取汇总，再取 DIMM 列表）----
        # show /System/Memory 包含 installed_memory / installed_dimms / max_dimms
        mem_summary = {}
        try:
            mem_summary = parse_props(cmd_memory(client))
        except Exception: pass
        raw_dimm_list = cmd_dimm_list(client)
        # 合并：DIMM 父级属性也尝试补充
        dimm_parent_props = parse_props(raw_dimm_list)
        mem_summary.update({k: v for k, v in dimm_parent_props.items() if k not in mem_summary or mem_summary[k] == "N/A"})
        data["memory"]["summary"] = mem_summary

        dimm_ids = parse_targets(raw_dimm_list)
        for did in dimm_ids:
            try:
                d = parse_props(cmd_dimm(client, did)); d["_id"] = did
                data["memory"]["dimms"].append(d)
            except Exception as e:
                logger.warning("[%s] DIMM %s 失败: %s", host, did, e)
        logger.info("[%s] 内存 OK，%d DIMMs", host, len(data["memory"]["dimms"]))

        # ---- 电源 ----
        data["power"]["summary"] = parse_props(cmd_power(client))
        for pid in ("Power_Supply_0", "Power_Supply_1"):
            try:
                p = parse_props(cmd_psu(client, pid)); p["_id"] = pid
                data["power"]["psus"].append(p)
            except Exception as e:
                logger.warning("[%s] %s 失败: %s", host, pid, e)
        logger.info("[%s] 电源 OK", host)

        # ---- 冷却 ----
        data["cooling"]["summary"] = parse_props(cmd_cooling(client))
        for i in range(12):
            fid = f"Fan_{i}"
            try:
                f = parse_props(cmd_fan(client, fid)); f["_id"] = fid
                f["fan_percentage_num"] = _fan_pct(f)
                f["fan_rpm_num"]        = _fan_rpm(f)
                data["cooling"]["fans"].append(f)
            except Exception as e:
                logger.warning("[%s] %s 失败: %s", host, fid, e)
        logger.info("[%s] 冷却 OK，%d 风扇", host, len(data["cooling"]["fans"]))

        # ---- 存储 ----
        data["storage"]["summary"] = parse_props(cmd_storage(client))
        disk_ids = parse_targets(cmd_disk_list(client))
        for did in disk_ids:
            try:
                dk = parse_props(cmd_disk(client, did)); dk["_id"] = did
                data["storage"]["disks"].append(dk)
            except Exception as e:
                logger.warning("[%s] Disk %s 失败: %s", host, did, e)
        logger.info("[%s] 存储 OK，%d 磁盘", host, len(data["storage"]["disks"]))

        # ---- 网络 ----
        data["networking"]["summary"] = parse_props(cmd_networking(client))
        try:
            n = parse_props(cmd_nic(client)); n["_id"] = "Ethernet_NIC_0"
            data["networking"]["nics"].append(n)
        except Exception as e:
            logger.warning("[%s] NIC 失败: %s", host, e)

        # ---- PCI ----
        try:
            pci_ids = parse_targets(cmd_pci_list(client))
            for did in pci_ids:
                try:
                    dev = parse_props(cmd_pci_dev(client, did)); dev["_id"] = did
                    data["pci_devices"].append(dev)
                except Exception as e:
                    logger.warning("[%s] PCI %s 失败: %s", host, did, e)
        except Exception as e:
            logger.warning("[%s] PCI 列表失败: %s", host, e)

        # ---- 事件日志 ----
        try:
            data["events"] = parse_events(cmd_event_log(client))
        except Exception as e:
            logger.warning("[%s] 事件日志失败: %s", host, e)
        logger.info("[%s] 事件 OK，%d 条", host, len(data["events"]))

        # ==== 新增采集 ====

        # ---- 开放问题 ----
        try:
            raw_probs = cmd_open_problems(client)
            prob_ids  = parse_targets(raw_probs)
            for pid in prob_ids:
                try:
                    pb = parse_props(cmd_open_problem(client, pid)); pb["_id"] = pid
                    data["open_problems"].append(pb)
                except Exception: pass
        except Exception as e:
            logger.warning("[%s] Open_Problems 失败: %s", host, e)
        logger.info("[%s] 开放问题 %d 条", host, len(data["open_problems"]))

        # ---- SP 信息 ----
        try:
            data["sp"] = parse_props(cmd_sp(client))
        except Exception as e:
            logger.warning("[%s] SP 失败: %s", host, e)

        try:
            data["sp_network"] = parse_props(cmd_sp_network(client))
        except Exception as e:
            logger.warning("[%s] SP/Network 失败: %s", host, e)

        try:
            data["sp_clock"] = parse_props(cmd_sp_clock(client))
        except Exception as e:
            logger.warning("[%s] SP/Clock 失败: %s", host, e)

        # ---- BIOS & 主板 ----
        try:
            data["bios"] = parse_props(cmd_bios(client))
        except Exception as e:
            logger.warning("[%s] BIOS 失败: %s", host, e)

        try:
            data["board_mb"] = parse_props(cmd_board_mb(client))
        except Exception as e:
            logger.warning("[%s] 主板 失败: %s", host, e)

        # ---- LED 指示灯 ----
        # ODA X11 ILOM 5.x: LED 状态在 /System 属性里（locator_indicator, power_state）
        # /SP/faultled、/System/Indicators 路径均不存在，不需要单独采集
        # data["system"] 已在上方采集完成，渲染时直接读取即可
        logger.info("[%s] 指示灯从 /System 属性读取", host)

        # ---- 传感器（自动探索 /SP/Sensors 结构）----
        # 先获取顶层，判断是「子目录模式」还是「直接传感器模式」
        _SENSOR_SUBCATS = {"temperature","temp","voltage","volt","fan","power","current"}
        try:
            _top_targets = parse_targets(client.run("show /SP/Sensors"))
            logger.info("[%s] /SP/Sensors 顶层: %s", host, _top_targets)
            _has_subcats = any(t.lower() in _SENSOR_SUBCATS for t in _top_targets)

            def _load_cat(cat_path: str, lst: list):
                try:
                    _ids = parse_targets(client.run(f"show {cat_path}"))
                    logger.info("[%s] %s: %d 个传感器", host, cat_path, len(_ids))
                    for _sid in _ids[:30]:
                        try:
                            _s = parse_props(client.run(f"show {cat_path}/{_sid}"))
                            _s["_id"] = _sid
                            lst.append(_s)
                        except Exception:
                            pass
                except Exception as _e2:
                    logger.warning("[%s] %s 失败: %s", host, cat_path, _e2)

            if _has_subcats:
                # 有子目录（Temperature/Voltage/Fan 等）
                for _cat_raw in _top_targets:
                    _cat_lo = _cat_raw.lower()
                    if _cat_lo in ("temperature", "temp"):
                        _load_cat(f"/SP/Sensors/{_cat_raw}", data["sensors_temp"])
                    elif _cat_lo in ("voltage", "volt"):
                        _load_cat(f"/SP/Sensors/{_cat_raw}", data["sensors_volt"])
                    elif _cat_lo == "fan":
                        _load_cat(f"/SP/Sensors/{_cat_raw}", data.setdefault("sensors_fan", []))
            else:
                # 直接传感器列表（无子目录）
                for _sid in _top_targets[:60]:
                    try:
                        _s = parse_props(client.run(f"show /SP/Sensors/{_sid}"))
                        _s["_id"] = _sid
                        _lo = _sid.lower()
                        _vs = (_s.get("currentreading","") + " " + _s.get("value","")).lower()
                        if "degree" in _vs or any(k in _lo for k in ("temp","t_","thermal")):
                            data["sensors_temp"].append(_s)
                        elif any(k in _lo for k in ("volt","v_")):
                            data["sensors_volt"].append(_s)
                        elif "rpm" in _vs or "fan" in _lo:
                            data.setdefault("sensors_fan",[]).append(_s)
                        else:
                            data["sensors_temp"].append(_s)
                    except Exception:
                        pass
        except Exception as _e:
            logger.warning("[%s] /SP/Sensors 探索失败: %s", host, _e)
        logger.info("[%s] 温度传感器 %d 个, 电压 %d 个", host,
                    len(data["sensors_temp"]), len(data["sensors_volt"]))

        # 将 SP 风扇传感器 RPM 回填到 cooling fans
        sp_fans = data.get("sensors_fan", [])
        for sp_f in sp_fans:
            sp_id = sp_f.get("_id", "").lower()
            cr    = _prop(sp_f, "current_reading","currentreading","reading","value")
            rpm   = parse_number(cr) if cr and "rpm" in cr.lower() else None
            if rpm:
                for cf in data["cooling"]["fans"]:
                    cf_id = cf.get("_id","").lower().replace("_","")
                    if cf_id in sp_id.replace("_",""):
                        cf["fan_rpm_num"] = rpm
                        break

        # ---- 固件版本 ----
        try:
            data["firmware"] = parse_props(cmd_firmware(client))
        except Exception as e:
            logger.warning("[%s] Firmware 失败: %s", host, e)

        # ---- 故障管理 ----
        try:
            data["fault_mgmt"] = parse_props(cmd_fault_mgmt(client))
        except Exception as e:
            logger.warning("[%s] FaultMgmt 失败: %s", host, e)

    except Exception as e:
        logger.error("[%s] 节点采集异常: %s\n%s", host, e, traceback.format_exc())
        data["error"] = str(e)
    finally:
        client.disconnect()
    return data


# ============================================================
# 指标摘要
# ============================================================
def compute_metrics(nd: dict) -> dict:
    cs = nd["cooling"]["summary"]; sys = nd["system"]; ps = nd["power"]["summary"]
    inlet   = parse_number(_prop(cs, "inlet_temp"))
    exhaust = parse_number(_prop(cs, "exhaust_temp"))
    power   = parse_number(_prop(sys, "actual_power_consumption") or
                           _prop(ps,  "actual_power_consumption"))
    health  = _prop(sys, "health", default="Unknown")
    fan_pcts = [f["fan_percentage_num"] for f in nd["cooling"]["fans"]
                if f.get("fan_percentage_num") is not None]
    return {
        "inlet_temp": inlet, "exhaust_temp": exhaust,
        "actual_power_consumption": power,
        "fan_percentage_avg": (sum(fan_pcts)/len(fan_pcts)) if fan_pcts else None,
        "system_health": health,
    }

def diff_label(cur: Optional[float], prev: Optional[float]) -> str:
    if cur is None or prev is None: return ""
    d = cur - prev
    if abs(d) < 0.05: return ""
    return f"{'↑' if d > 0 else '↓'}{abs(d):.1f}"


# ============================================================
# HTML 辅助（MySQL 巡检报告风格）
# ============================================================
CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Microsoft YaHei","PingFang SC",Arial,sans-serif;background:#f5f5f5;color:#333;font-size:14px}
a{text-decoration:none}

/* 顶部标题栏 */
.hdr{background:linear-gradient(135deg,#0d2b4e,#1565C0);color:#fff;padding:24px 40px}
.hdr h1{font-size:22px;margin-bottom:6px;font-weight:700;text-align:center}
.hdr p{opacity:.9;font-size:13px;text-align:center}

/* 粘性导航 */
nav{position:sticky;top:0;z-index:100;background:#1565C0;display:flex;gap:0;
    padding:0 20px;box-shadow:0 2px 6px rgba(0,0,0,.25);overflow-x:auto}
nav a{color:#e3f2fd;padding:11px 13px;font-size:13px;white-space:nowrap;
      transition:background .2s;display:block}
nav a:hover{background:#0d47a1;color:#fff}

.wrap{max-width:1700px;margin:20px auto;padding:0 20px}

/* 执行摘要卡片 */
.sum-cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));
           gap:16px;margin-bottom:20px}
.sum-card{background:#fff;border-radius:8px;padding:18px 20px;
          box-shadow:0 1px 4px rgba(0,0,0,.1);border-left:4px solid #1976D2}
.sum-card.warn{border-left-color:#e65100}
.sum-card.crit{border-left-color:#b71c1c}
.sum-card-title{font-size:11px;color:#555;font-weight:600;text-transform:uppercase;
                letter-spacing:.5px;margin-bottom:10px}
.sum-val{font-size:24px;font-weight:700;color:#1a1a1a}
.sum-sub{font-size:12px;color:#555;margin-top:3px}

/* Section 卡片 */
.sec{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.1);
     margin-bottom:20px;overflow:hidden}

/* Section 标题 —— MySQL h1 风格：蓝色文字 + 下边框 */
.sec-hdr{color:#1565C0;font-size:16px;font-weight:700;
         padding:16px 22px 12px;border-bottom:2px solid #e3f2fd;
         display:flex;align-items:center;justify-content:space-between}
.sec-hdr .right{font-size:12px;color:#666;font-weight:400}
.sec-body{padding:0}

/* 子节标题 —— MySQL h2 风格：左边框 + 蓝色 */
.sub-sec{padding:14px 22px 6px 18px;font-weight:600;color:#1976D2;font-size:14px;
         border-left:4px solid #1976D2;margin-left:22px;
         border-top:none}
.sub-sec:first-child{margin-top:4px}

/* KV 网格 */
.kv-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));
         gap:14px;padding:14px 22px}
.kv-item .lbl{font-size:11px;color:#555;margin-bottom:3px}
.kv-item .val{font-size:15px;font-weight:700;color:#1a1a1a}
.kv-item .diff{font-size:12px;color:#e65100;margin-left:4px}

/* 表格 —— MySQL 蓝色表头 */
table{width:100%;border-collapse:collapse}
th{background:#1565C0;color:#fff;font-weight:600;padding:9px 13px;
   text-align:left;font-size:13px;white-space:nowrap}
td{padding:8px 13px;border:1px solid #e0e0e0;font-size:13px;
   color:#333;vertical-align:middle}
tr:nth-child(even) td{background:#f9f9f9}
tr:hover td{background:#e3f2fd}
.row-major td{background:#ffebee!important;color:#b71c1c}
.row-minor td{background:#fff8e1!important}
.tbl-wrap{overflow-x:auto;padding:0 22px 14px}

/* 状态徽标 */
.badge-ok  {background:#e8f5e9;color:#2e7d32;border-radius:4px;padding:2px 8px;font-size:12px;font-weight:600}
.badge-warn{background:#fff3e0;color:#bf360c;border-radius:4px;padding:2px 8px;font-size:12px;font-weight:600}
.badge-crit{background:#ffebee;color:#b71c1c;border-radius:4px;padding:2px 8px;font-size:12px;font-weight:600}
.badge-info{background:#e3f2fd;color:#1565C0;border-radius:4px;padding:2px 8px;font-size:12px;font-weight:600}

/* 进度条 */
.bar-wrap{position:relative;background:#e0e0e0;border-radius:20px;
          height:20px;overflow:hidden;min-width:80px}
.bar{height:100%;border-radius:20px;transition:width .3s}
.bar-txt{position:absolute;top:50%;left:50%;
         transform:translate(-50%,-50%);font-size:11px;font-weight:700;
         color:#fff;white-space:nowrap;
         text-shadow:1px 1px 2px rgba(0,0,0,.6),-1px -1px 2px rgba(0,0,0,.6)}

/* 温度 */
.temp-hi{color:#b71c1c;font-weight:700}
.temp-ok{color:#2e7d32;font-weight:600}

/* PSU 卡片 */
.psu-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:10px 22px 16px}
.psu-card{border:1px solid #e0e0e0;border-radius:6px;padding:14px 16px;background:#fafafa}
.psu-card-title{font-size:13px;color:#1976D2;font-weight:600;margin-bottom:10px;
                border-left:3px solid #1976D2;padding-left:8px}
.psu-metric{display:flex;justify-content:space-between;align-items:center;
            padding:5px 0;border-bottom:1px solid #f0f0f0}
.psu-metric:last-child{border-bottom:none}
.psu-metric .mkey{font-size:12px;color:#555}
.psu-metric .mval{font-weight:700;color:#1a1a1a}

/* 指示灯 */
.led-on  {color:#2e7d32;font-weight:700}
.led-off {color:#757575}
.led-fault{color:#b71c1c;font-weight:700}

/* 图表 */
.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;padding:16px 22px 20px}
.chart-box{background:#f8f9fa;border-radius:6px;padding:16px;border:1px solid #e0e0e0}
.chart-label{font-weight:600;color:#1565C0;margin-bottom:10px;font-size:13px}

/* 巡检结论横幅 */
.conclusion-box{border-radius:8px;padding:16px 20px;margin:0 0 20px;
                display:flex;align-items:center;gap:14px;border-left:5px solid}
.conclusion-icon{font-size:1.9em}
.conclusion-text{font-size:1.1em;font-weight:700}
.conclusion-sub{font-size:12px;color:#555;margin-top:4px}

/* 页脚 */
.foot{text-align:center;color:#999;font-size:12px;padding:20px 0 30px;border-top:1px solid #eee;margin-top:4px}
.na{color:#888}
.err{color:#b71c1c}
"""

def _hbadge(h: str) -> str:
    hl = (h or "").lower()
    if hl == "ok":       return f"<span class='badge-ok'>{h}</span>"
    if hl in ("warning","minor"): return f"<span class='badge-warn'>{h}</span>"
    if hl in ("critical","major","fault"): return f"<span class='badge-crit'>{h}</span>"
    if not h or h == "N/A": return "<span class='na'>N/A</span>"
    return f"<span class='badge-info'>{h}</span>"

def _pbar(val: Optional[float], warn: float = 80, max_w: str = "200px") -> str:
    if val is None: return "<span class='na'>N/A</span>"
    pct = min(max(val, 0), 100)
    color = "#b71c1c" if val >= warn else ("#e65100" if val >= 60 else "#388E3C")
    return (
        f"<div class='bar-wrap' style='max-width:{max_w}'>"
        f"<div class='bar' style='width:{pct:.1f}%;background:{color}'></div>"
        f"<span class='bar-txt'>{val:.0f}%</span>"
        f"</div>"
    )

def _pbar_power(actual: Optional[float], max_w: Optional[float], bar_w: str = "220px") -> str:
    if actual is None: return "<span class='na'>N/A</span>"
    if max_w and max_w > 0:
        pct   = min(actual / max_w * 100, 100)
        color = "#b71c1c" if pct >= 80 else ("#e65100" if pct >= 60 else "#388E3C")
        txt   = f"{actual:.0f}W / {max_w:.0f}W ({pct:.0f}%)"
    else:
        pct   = 0; color = "#388E3C"; txt = f"{actual:.0f}W"
    return (
        f"<div class='bar-wrap' style='max-width:{bar_w}'>"
        f"<div class='bar' style='width:{pct:.1f}%;background:{color}'></div>"
        f"<span class='bar-txt'>{txt}</span>"
        f"</div>"
    )

def _temp_cls(val: Optional[float], threshold: float = 40) -> str:
    if val is None: return "<span class='na'>N/A</span>"
    cls = "temp-hi" if val >= threshold else "temp-ok"
    return f"<span class='{cls}'>{val:.1f}°C</span>"


# ---- 子卡片渲染 ----

def _tbl(headers: list, rows_html: str, wrap: bool = True) -> str:
    ths = "".join(f"<th>{h}</th>" for h in headers)
    tbl = f"<table><tr>{ths}</tr>{rows_html}</table>"
    return f"<div class='tbl-wrap'>{tbl}</div>" if wrap else tbl

def _card_cpu(cpus: dict) -> str:
    rows = ""
    for cid, c in cpus.items():
        if c.get("_error"):
            rows += f"<tr><td>{cid}</td><td colspan='5' class='err'>采集失败: {c['_error']}</td></tr>"
            continue
        h = _prop(c, "health")
        rows += (
            f"<tr><td><b>{cid}</b></td>"
            f"<td>{_hbadge(h)}</td>"
            f"<td>{_prop(c,'location')}</td>"
            f"<td>{_prop(c,'model')}</td>"
            f"<td>{_prop(c,'max_clock_speed')}</td>"
            f"<td>{_prop(c,'total_cores','N/A')} / {_prop(c,'enabled_cores','N/A')}</td></tr>"
        )
    return _tbl(["编号","健康","位置","型号","主频","总核心/启用核心"], rows)

def _card_memory(mem: dict) -> str:
    s = mem["summary"]
    total    = _prop(s, "installed_memory","total_memory","memory_capacity","total_capacity","size")
    inst_cnt = _prop(s, "installed_dimms","dimm_count","populated_dimms","installed","count")
    max_cnt  = _prop(s, "max_dimms","maximum_dimms","max_count","max_memory_modules")
    rows = ""
    for d in mem["dimms"]:
        h = _prop(d, "health")
        rows += (
            f"<tr><td><b>{d.get('_id','')}</b></td>"
            f"<td>{_prop(d,'location')}</td>"
            f"<td>{_prop(d,'memory_size','size','capacity')}</td>"
            f"<td>{_prop(d,'type','memory_type','dram_type')}</td>"
            f"<td>{_prop(d,'manufacturer','vendor')}</td>"
            f"<td>{_hbadge(h)}</td></tr>"
        )
    kv = (
        f"<div class='kv-grid'>"
        f"<div class='kv-item'><div class='lbl'>总容量</div><div class='val'>{total}</div></div>"
        f"<div class='kv-item'><div class='lbl'>已安装 DIMM</div><div class='val'>{inst_cnt}</div></div>"
        f"<div class='kv-item'><div class='lbl'>最大 DIMM 数</div><div class='val'>{max_cnt}</div></div>"
        f"</div>"
    )
    return kv + _tbl(["DIMM","位置","容量","类型","厂商","健康"], rows)

def _card_power(pwr: dict) -> str:
    s      = pwr["summary"]
    h      = _prop(s, "health")
    actual = parse_number(_prop(s, "actual_power_consumption"))
    max_w  = parse_number(_prop(s, "max_permitted_power"))
    inst   = _prop(s, "installed_power_supplies")

    # 总功耗进度条
    top = (
        f"<div class='kv-grid'>"
        f"<div class='kv-item'><div class='lbl'>健康</div><div class='val'>{_hbadge(h)}</div></div>"
        f"<div class='kv-item'><div class='lbl'>已安装 PSU</div><div class='val'>{inst}</div></div>"
        f"<div class='kv-item' style='grid-column:1/-1'>"
        f"<div class='lbl'>总功耗</div>"
        f"{_pbar_power(actual, max_w, '260px')}"
        f"</div></div>"
    )

    # 每个 PSU 独立卡片
    psu_cards = "<div class='psu-grid'>"
    for p in pwr["psus"]:
        ph  = _prop(p, "health")
        inp = _prop(p, "input_power","ac_input_power","input_watts","input")
        out = _prop(p, "output_power","dc_output_power","output_watts","output")
        inp_n = parse_number(inp); out_n = parse_number(out)

        # ILOM 只报 AC 在线状态（"Present"），无法计算效率
        if inp and inp.lower() in ("present","ac present","yes","on"):
            inp_disp = "<span style='color:#2e7d32;font-weight:700'>✅ AC在线</span>"
            eff_row  = ""
        elif inp_n and inp_n > 0:
            inp_disp = inp
            eff_row  = (f"<div class='psu-metric'>"
                        f"<span class='mkey'>转换效率</span>"
                        f"<span class='mval'>{out_n/inp_n*100:.1f}%</span>"
                        f"</div>") if out_n else ""
        else:
            inp_disp = "—"; eff_row = ""

        out_disp = out if out != "N/A" else "—"
        psu_cards += (
            f"<div class='psu-card'>"
            f"<div class='psu-card-title'>{p.get('_id','PSU')} — {_prop(p,'location')}</div>"
            f"<div style='margin-bottom:8px'>{_hbadge(ph)}</div>"
            f"<div class='psu-metric'><span class='mkey'>输入功率 (AC)</span>"
            f"<span class='mval'>{inp_disp}</span></div>"
            f"<div class='psu-metric'><span class='mkey'>输出功率 (DC)</span>"
            f"<span class='mval' style='color:#1565C0;font-size:1.05em'>{out_disp}</span></div>"
            f"{eff_row}"
            f"<div class='psu-metric'><span class='mkey'>厂商</span>"
            f"<span class='mval'>{_prop(p,'manufacturer','vendor')}</span></div>"
            f"<div class='psu-metric'><span class='mkey'>零件号</span>"
            f"<span class='mval'>{_prop(p,'part_number')}</span></div>"
            f"<div class='psu-metric'><span class='mkey'>序列号</span>"
            f"<span class='mval'>{_prop(p,'serial_number','serialnumber')}</span></div>"
            f"</div>"
        )
    psu_cards += "</div>"
    return top + psu_cards

def _card_cooling(cool: dict) -> str:
    s       = cool["summary"]
    fans    = cool["fans"]
    inlet   = parse_number(_prop(s, "inlet_temp"))
    exhaust = parse_number(_prop(s, "exhaust_temp"))
    h       = _prop(s, "health")
    fan_ids = cool["fans"]

    top = (
        f"<div class='kv-grid'>"
        f"<div class='kv-item'><div class='lbl'>健康</div><div class='val'>{_hbadge(h)}</div></div>"
        f"<div class='kv-item'><div class='lbl'>入口温度</div><div class='val'>{_temp_cls(inlet,40)}</div></div>"
        f"<div class='kv-item'><div class='lbl'>排风温度</div><div class='val'>{_temp_cls(exhaust,40)}</div></div>"
        f"<div class='kv-item'><div class='lbl'>已安装风扇</div><div class='val'>{_prop(s,'installed_chassis_fans','fan_count')}</div></div>"
        f"</div>"
    )
    has_rpm = any(f.get("fan_rpm_num") is not None for f in fans)
    rows = ""
    for f in fans:
        fh  = _prop(f, "health")
        pct = f.get("fan_percentage_num")
        rpm = f.get("fan_rpm_num")
        row = (
            f"<tr><td><b>{f.get('_id','')}</b></td>"
            f"<td>{_prop(f,'location')}</td>"
            f"<td>{_hbadge(fh)}</td>"
            f"<td>{_pbar(pct, 80, '180px')}</td>"
        )
        if has_rpm:
            row += f"<td>{f'{rpm:.0f} RPM' if rpm else '—'}</td>"
        row += "</tr>"
        rows += row
    headers = ["风扇","位置","健康","转速 %"] + (["RPM"] if has_rpm else [])
    return top + _tbl(headers, rows)

def _card_storage(stor: dict) -> str:
    s = stor["summary"]; h = _prop(s, "health")
    top = (
        f"<div class='kv-grid'>"
        f"<div class='kv-item'><div class='lbl'>健康</div><div class='val'>{_hbadge(h)}</div></div>"
        f"<div class='kv-item'><div class='lbl'>已安装磁盘</div><div class='val'>{_prop(s,'installed_disks','disk_count')}</div></div>"
        f"<div class='kv-item'><div class='lbl'>最大磁盘数</div><div class='val'>{_prop(s,'max_disks','maximum_disks')}</div></div>"
        f"<div class='kv-item'><div class='lbl'>磁盘控制器</div><div class='val'>{_prop(s,'disk_controllers','controllers')}</div></div>"
        f"</div>"
    )
    rows = ""
    for d in stor["disks"]:
        dh = _prop(d, "health")
        rows += (
            f"<tr><td><b>{d.get('_id','')}</b></td>"
            f"<td>{_prop(d,'location')}</td>"
            f"<td>{_prop(d,'type','disk_type','media_type')}</td>"
            f"<td>{_hbadge(dh)}</td></tr>"
        )
    return top + _tbl(["磁盘","位置","类型","健康"], rows)

def _card_networking(net: dict) -> str:
    s = net["summary"]; h = _prop(s, "health")
    top = (
        f"<div class='kv-grid'>"
        f"<div class='kv-item'><div class='lbl'>健康</div><div class='val'>{_hbadge(h)}</div></div>"
        f"<div class='kv-item'><div class='lbl'>已安装 NIC</div><div class='val'>{_prop(s,'installed_eth_nics','nic_count')}</div></div>"
        f"</div>"
    )
    rows = ""
    for n in net["nics"]:
        rows += (
            f"<tr><td>{n.get('_id','')}</td>"
            f"<td>{_prop(n,'manufacturer','vendor')}</td>"
            f"<td>{_prop(n,'part_number','model')}</td>"
            f"<td>{_prop(n,'mac_addresses','mac_address')}</td></tr>"
        )
    return top + _tbl(["NIC","厂商","型号","MAC地址"], rows)

def _card_pci(devs: list) -> str:
    rows = ""
    for d in devs:
        rows += (
            f"<tr><td>{d.get('_id','')}</td>"
            f"<td>{_prop(d,'description','device_description')}</td>"
            f"<td>{_prop(d,'location','slot')}</td>"
            f"<td>{_prop(d,'pci_vendor_id','vendor_id')}</td>"
            f"<td>{_prop(d,'pci_device_id','device_id')}</td></tr>"
        )
    return _tbl(["设备","描述","位置","厂商ID","设备ID"], rows)

def _sec_node(nd: dict) -> str:
    sys_s = nd.get("system", {}); h = _prop(sys_s, "health", default="Unknown")
    icon  = {"ok": "✅", "warning": "⚠️"}.get(h.lower(), "❌")
    hdr   = (
        f"<div class='sec-hdr'>"
        f"<span>{icon} {nd['node_name']} ({nd['host']}) — 硬件详情</span>"
        f"<span class='right'>型号: {_prop(sys_s,'model')} | "
        f"SN: {_prop(sys_s,'system_identifier')} | "
        f"固件: {_prop(sys_s,'system_fw_version')}</span>"
        f"</div>"
    )
    err = (f"<div style='padding:12px 22px;color:#b71c1c;font-weight:600'>"
           f"⚠️ 采集错误: {nd['error']}</div>") if nd.get("error") else ""
    body = (
        f"<div class='sub-sec'>处理器</div>{_card_cpu(nd.get('cpus',{}))}"
        f"<div class='sub-sec'>内存</div>{_card_memory(nd.get('memory',{}))}"
        f"<div class='sub-sec'>电源</div>{_card_power(nd.get('power',{}))}"
        f"<div class='sub-sec'>冷却</div>{_card_cooling(nd.get('cooling',{}))}"
        f"<div class='sub-sec'>存储</div>{_card_storage(nd.get('storage',{}))}"
        f"<div class='sub-sec'>网络</div>{_card_networking(nd.get('networking',{}))}"
        f"<div class='sub-sec'>PCI 设备</div>{_card_pci(nd.get('pci_devices',[]))}"
    )
    return f"<div class='sec'>{hdr}<div class='sec-body'>{err}{body}</div></div>"


# _sec_sensors 已移除：ODA X11 ILOM 5.x 不暴露 /SP/Sensors 路径，
# 入口/排风温度已在冷却章节显示，CPU 温度标注为 Not Supported。


# ---- SP / ILOM 信息 ----
def _parse_sp_desc(desc: str) -> dict:
    """从 ILOM description 字符串提取结构化字段。
    示例: 'ORACLE SERVER E6-2L, ILOM v5.1.4.23, r161249'
    """
    result = {"raw": desc}
    if not desc or desc == "N/A":
        return result
    parts = [p.strip() for p in desc.split(",")]
    if parts:
        result["server_model"] = parts[0]
    for part in parts:
        m = re.search(r"ILOM\s+(v[\d.]+)", part, re.IGNORECASE)
        if m: result["ilom_version"] = m.group(1)
        m = re.search(r"\br(\d{5,})\b", part)
        if m: result["build_rev"] = m.group(1)
    return result

def _sec_sp(all_nd: list) -> str:
    content = ""
    for nd in all_nd:
        nm  = nd["node_name"]
        sp  = nd.get("sp", {}); spn = nd.get("sp_network", {})
        spc = nd.get("sp_clock", {}); bios = nd.get("bios", {})
        mb  = nd.get("board_mb", {}); fw = nd.get("firmware", {})
        sys_s = nd.get("system", {})  # /System 属性也有部分系统信息

        # 辅助：只输出有值的行
        def _row(label, val):
            if not val or val == "N/A": return ""
            return (f"<tr><td style='width:200px;color:#555;vertical-align:top'>{label}</td>"
                    f"<td><b>{val}</b></td></tr>")

        # 解析 SP description 获取型号/固件
        desc_parsed = _parse_sp_desc(_prop(sp, "description"))
        sp_model = (_prop(sp, "model","product_name","name")
                    if _prop(sp,"model","product_name","name") != "N/A"
                    else desc_parsed.get("server_model","N/A"))
        sp_fw    = (_prop(sp, "firmware_version","sp_fw_version","version","fw_version")
                    if _prop(sp,"firmware_version","sp_fw_version","version","fw_version") != "N/A"
                    else desc_parsed.get("ilom_version","N/A"))
        sp_sn    = _prop(sp, "serialnumber","serial_number","sn","sp_serial")
        sp_host  = _prop(sp, "current_hostname","hostname","host","name")
        build_rev= desc_parsed.get("build_rev","")

        # SP / ILOM 基本信息
        sp_rows = ""
        sp_rows += _row("服务器型号",  sp_model)
        sp_rows += _row("ILOM 版本",   sp_fw + (f"  (r{build_rev})" if build_rev else ""))
        sp_rows += _row("SP 序列号",   sp_sn)
        sp_rows += _row("主机名",      sp_host)
        sp_rows += _row("完整描述",    desc_parsed.get("raw",""))

        # 网络 —— ILOM 5.x 字段名无下划线/前缀
        sp_rows += _row("ILOM IP",
            _prop(spn, "ipaddress","pendingipaddress","committedaddress",
                       "ipv4address","ip_address","pending_ipv4_address"))
        sp_rows += _row("子网掩码",
            _prop(spn, "ipnetmask","pendingipnetmask","ipv4netmask",
                       "ip_netmask","ipv4_subnet_mask","subnet_mask"))
        sp_rows += _row("默认网关",
            _prop(spn, "ipgateway","pendingipgateway","gateway",
                       "ipv4_gateway","pending_ipv4_gateway"))
        sp_rows += _row("IP 分配方式",
            _prop(spn, "ipdiscovery","pendingipdiscovery","ipv4assignment",
                       "ipv4_assignment","ip_assignment","assignment"))
        sp_rows += _row("MAC 地址",
            _prop(spn, "macaddress","mac_address","mac","sp_mac_address"))
        sp_rows += _row("管理端口",
            _prop(spn, "managementport","management_port","port"))

        # 时间/NTP（/SP/clock 字段）
        sp_rows += _row("当前时间",
            _prop(spc, "datetime","current_time","time"))
        sp_rows += _row("时区",
            _prop(spc, "timezone","tz"))
        sp_rows += _row("系统运行时长",
            _prop(spc, "uptime"))
        sp_rows += _row("NTP 已启用",
            _prop(spc, "usentpserver","ntp_status","ntp","ntpenabled","ntp_enabled"))
        sp_rows += _row("NTP 服务器",
            _prop(spc, "ntpservers","ntpserver","ntp_server","ntp_servers"))

        # 硬件版本
        hw_rows = ""
        hw_rows += _row("BIOS 版本",
            _prop(bios, "version","bios_version","firmware_version"))
        hw_rows += _row("BIOS 日期",
            _prop(bios, "build_date","releasedatetime","releasedate",
                        "release_date","builddate","date"))
        hw_rows += _row("固件包版本",
            _prop(fw, "version","firmware_version","system_version"))
        hw_rows += _row("系统型号",
            _prop(sys_s, "model","product_name"))
        hw_rows += _row("系统序列号",   _prop(sys_s, "serial_number","serialnumber","sn"))
        hw_rows += _row("系统标识",     _prop(sys_s, "system_identifier"))
        hw_rows += _row("系统零件号",
            _prop(sys_s, "part_number","partnumber","pn"))
        # 主板（路径可能不可用，有值才显示）
        hw_rows += _row("主板型号",   _prop(mb, "model","product_name"))
        hw_rows += _row("主板序列号", _prop(mb, "serial_number","serialnumber","sn"))
        hw_rows += _row("主板零件号", _prop(mb, "part_number","partnumber","pn"))

        # 指示灯 —— ODA X11 ILOM 5.x 在 /System 属性中暴露
        # locator_indicator / power_state / health（间接故障指示）
        led_rows = ""
        def _led_row(display, cls, label, raw):
            return (f"<tr><td style='width:220px;color:#555'>{display}</td>"
                    f"<td><span class='{cls}'>{label}</span>"
                    f"<span style='color:#888;font-size:12px;margin-left:6px'>({raw})</span></td></tr>")

        loc_v = sys_s.get("locator_indicator", "")
        if loc_v:
            if loc_v.lower() in ("on","lit","fast","slow"):
                led_rows += _led_row("定位灯 (LOCATE)", "led-on", "🔵 ON", loc_v)
            else:
                led_rows += _led_row("定位灯 (LOCATE)", "led-off", "OFF", loc_v)

        pwr_v = sys_s.get("power_state", "")
        if pwr_v:
            if pwr_v.lower() in ("on",):
                led_rows += _led_row("电源状态 (POWER)", "led-on", "🟢 On", pwr_v)
            else:
                led_rows += _led_row("电源状态 (POWER)", "led-off", pwr_v, pwr_v)

        hlth_v = sys_s.get("health", "")
        if hlth_v:
            if hlth_v.lower() == "ok":
                led_rows += _led_row("系统健康 (FAULT)", "led-off", "✅ OK", hlth_v)
            else:
                led_rows += _led_row("系统健康 (FAULT)", "led-fault", f"⛔ {hlth_v}", hlth_v)

        tbl_sp  = f"<div class='tbl-wrap'><table>{sp_rows}</table></div>"  if sp_rows  else ""
        tbl_hw  = f"<div class='tbl-wrap'><table>{hw_rows}</table></div>"  if hw_rows  else ""
        tbl_led = (f"<div class='tbl-wrap'><table>{led_rows}</table></div>" if led_rows
                   else "<div style='padding:10px 22px 14px;color:#888'>指示灯数据不可用</div>")

        content += (
            f"<div class='sub-sec'>{nm} — SP / ILOM 信息 &amp; 网络</div>{tbl_sp}"
            f"<div class='sub-sec'>{nm} — 硬件 &amp; 固件版本</div>{tbl_hw}"
            f"<div class='sub-sec'>{nm} — 指示灯状态</div>{tbl_led}"
        )
    return f"<div class='sec' id='sec-sp'><div class='sec-hdr'>ILOM / SP 信息</div><div class='sec-body'>{content}</div></div>"


# ---- 开放问题 ----
def _sec_problems(all_nd: list) -> str:
    all_probs = [(nd["node_name"], p) for nd in all_nd for p in nd.get("open_problems", [])]
    if not all_probs:
        body = "<div style='padding:20px;color:#2e7d32;text-align:center'>✅ 无开放故障问题</div>"
    else:
        rows = ""
        for nm, p in all_probs:
            sev = _prop(p, "severity")
            sev_cls = "badge-crit" if "major" in sev.lower() or "critical" in sev.lower() else "badge-warn"
            rows += (
                f"<tr><td>{nm}</td>"
                f"<td>{p.get('_id','')}</td>"
                f"<td>{_prop(p,'timestamp','time','datetime')}</td>"
                f"<td><span class='{sev_cls}'>{sev}</span></td>"
                f"<td>{_prop(p,'description','fault_description','summary')}</td>"
                f"<td>{_prop(p,'additional_details','details','recommended_action')}</td></tr>"
            )
        body = _tbl(["节点","ID","时间","严重度","描述","建议操作"], rows)
    cnt = len(all_probs)
    title = f"开放故障问题 ({cnt})" if cnt else "开放故障问题"
    return f"<div class='sec' id='sec-problems'><div class='sec-hdr'>{title}</div><div class='sec-body'>{body}</div></div>"


# ---- 历史趋势 ----
def _sec_trends(all_history: Dict[str, List[dict]]) -> str:
    all_dates = sorted({r["date"] for h in all_history.values() for r in h})
    if not all_dates:
        return (f"<div class='sec' id='sec-trend'>"
                f"<div class='sec-hdr'>历史趋势（近7天）</div>"
                f"<div style='padding:20px;color:#888;text-align:center'>暂无历史数据</div></div>")
    labels = json.dumps(all_dates)
    colors = ["#1976D2","#D32F2F","#388E3C","#F57C00"]

    def ds(name, field, color, label):
        hmap = {r["date"]: r.get(field) for r in all_history.get(name, [])}
        vals = [hmap.get(d) for d in all_dates]
        return (f"{{label:'{label}',data:{json.dumps(vals)},"
                f"borderColor:'{color}',backgroundColor:'{color}22',"
                f"tension:0.3,fill:false,spanGaps:true,pointRadius:4}}")

    opts = ("{"
            "responsive:true,maintainAspectRatio:false,"
            "plugins:{legend:{labels:{color:'#333',font:{size:12}}}},"
            "scales:{x:{ticks:{color:'#666'},grid:{color:'#f0f0f0'}},"
            "y:{ticks:{color:'#666'},grid:{color:'#f0f0f0'}}}"
            "}")
    charts = ""
    names = list(all_history.keys())
    for cid, title, field, unit in [
        ("cTemp",    "入口温度",    "inlet_temp",              "°C"),
        ("cExhaust", "排风温度",    "exhaust_temp",            "°C"),
        ("cPower",   "功耗",        "actual_power_consumption", "W"),
        ("cFan",     "风扇平均转速","fan_percentage_avg",       "%"),
    ]:
        datasets = [ds(n, field, colors[i%4], f"{n} ({unit})") for i,n in enumerate(names)]
        charts += (
            f"<div class='chart-box'>"
            f"<div class='chart-label'>{title} 趋势</div>"
            f"<div style='position:relative;height:200px'>"
            f"<canvas id='{cid}'></canvas></div></div>"
            f"<script>new Chart(document.getElementById('{cid}'),"
            f"{{type:'line',data:{{labels:{labels},datasets:[{','.join(datasets)}]}},"
            f"options:{opts}}});</script>"
        )
    return (f"<div class='sec' id='sec-trend'>"
            f"<div class='sec-hdr'>历史趋势（近7天）</div>"
            f"<div class='chart-grid'>{charts}</div></div>")


# ---- 事件日志 ----
def _sec_events(all_nd: list) -> str:
    all_ev = [e for nd in all_nd for e in nd.get("events", [])]
    rows = ""
    for e in all_ev:
        sev = e.get("severity", "")
        tr_cls = "row-major" if sev in ("major","critical") else ("row-minor" if sev == "minor" else "")
        sev_badge = (f"<span class='badge-crit'>{sev.upper()}</span>"
                     if sev in ("major","critical") else
                     f"<span class='badge-warn'>{sev.upper()}</span>"
                     if sev == "minor" else
                     f"<span class='badge-info'>{sev.upper()}</span>")
        rows += (
            f"<tr class='{tr_cls}'><td>{e.get('id','')}</td>"
            f"<td style='white-space:nowrap'>{e.get('datetime','')}</td>"
            f"<td>{e.get('class','')}</td><td>{e.get('type','')}</td>"
            f"<td>{sev_badge}</td>"
            f"<td>{e.get('description','')}</td></tr>"
        )
    major_cnt = sum(1 for e in all_ev if e.get("severity") in ("major","critical"))
    hdr_cls   = f" — <span style='background:#b71c1c;color:#fff;border-radius:10px;padding:1px 8px;font-size:12px'>{major_cnt} Major</span>" if major_cnt else ""
    body = _tbl(["ID","时间","Class","Type","Severity","描述"], rows) if rows else (
        "<div style='padding:20px;color:#888;text-align:center'>无事件记录</div>")
    return (f"<div class='sec' id='sec-events'>"
            f"<div class='sec-hdr'>事件日志（最近50条）{hdr_cls}</div>"
            f"<div class='sec-body'>{body}</div></div>")


# ---- 巡检结论 ----
def _sec_conclusion(all_nd: list, all_events: list) -> str:
    majors = [e for e in all_events if e.get("severity") in ("major","critical")]
    minors = [e for e in all_events if e.get("severity") == "minor"]
    unhealthy = any(
        (_prop(nd.get("system",{}), "health", default="") or "").lower() not in ("ok","","unknown")
        and _prop(nd.get("system",{}), "health", default="").lower() != "ok"
        for nd in all_nd
    )
    open_probs = [p for nd in all_nd for p in nd.get("open_problems", [])]
    if unhealthy or majors or open_probs:
        color, icon, text = "#ffebee", "❌", "需要立即处理"
        border = "#b71c1c"
    elif minors:
        color, icon, text = "#fff3e0", "⚠️", "存在待关注项"
        border = "#e65100"
    else:
        color, icon, text = "#e8f5e9", "✅", "系统运行正常"
        border = "#2e7d32"
    return (
        f"<div class='conclusion-box' style='background:{color};border-left:6px solid {border}'>"
        f"<div class='conclusion-icon'>{icon}</div>"
        f"<div><div class='conclusion-text' style='color:{border}'>巡检结论：{text}</div>"
        f"<div class='conclusion-sub'>"
        f"开放问题: {len(open_probs)} | Major事件: {len(majors)} | Minor事件: {len(minors)}"
        f"</div></div></div>"
    )


# ============================================================
# 执行摘要卡片
# ============================================================
def _summary_cards(all_nd: list, all_metrics: dict, all_yesterday: dict) -> str:
    all_events = [e for nd in all_nd for e in nd.get("events", [])]
    major_cnt  = sum(1 for e in all_events if e.get("severity") in ("major","critical"))
    minor_cnt  = sum(1 for e in all_events if e.get("severity") == "minor")

    cards = ""
    for nd in all_nd:
        nm = nd["node_name"]; m = all_metrics.get(nm, {}); y = all_yesterday.get(nm) or {}
        h  = _prop(nd.get("system",{}), "health", default="Unknown")
        hl = h.lower()
        card_cls = "sum-card" + ("" if hl == "ok" else (" warn" if hl == "warning" else " crit"))
        icon   = "✅" if hl == "ok" else ("⚠️" if hl == "warning" else "❌")
        cards += (
            f"<div class='{card_cls}'>"
            f"<div class='sum-card-title'>{nm}</div>"
            f"<div class='sum-val'>{icon} {h}</div>"
            f"<div class='sum-sub'>"
            + (f"入口 {m.get('inlet_temp',0):.1f}°C{(' '+diff_label(m.get('inlet_temp'),y.get('inlet_temp'))) if diff_label(m.get('inlet_temp'),y.get('inlet_temp')) else ''}"
               if m.get("inlet_temp") is not None else "入口 N/A")
            + (f" | 排风 {m.get('exhaust_temp',0):.1f}°C" if m.get("exhaust_temp") is not None else "")
            + f"</div>"
            f"<div class='sum-sub'>"
            + (f"功耗 {m.get('actual_power_consumption',0):.0f}W" if m.get("actual_power_consumption") is not None else "功耗 N/A")
            + (f" | 风扇 {m.get('fan_percentage_avg',0):.0f}%" if m.get("fan_percentage_avg") is not None else "")
            + f"</div>"
            f"</div>"
        )
    # 事件卡
    ev_cls = "sum-card crit" if major_cnt else ("sum-card warn" if minor_cnt else "sum-card")
    cards += (
        f"<div class='{ev_cls}'>"
        f"<div class='sum-card-title'>事件统计</div>"
        f"<div class='sum-val' style='color:{'#b71c1c' if major_cnt else '#2e7d32'}'>"
        f"{'⛔' if major_cnt else '✅'} {major_cnt} Major</div>"
        f"<div class='sum-sub'>Minor: {minor_cnt} 条</div>"
        f"<div class='sum-sub'>开放问题: {sum(len(nd.get('open_problems',[])) for nd in all_nd)}</div>"
        f"</div>"
    )
    return f"<div class='sum-cards'>{cards}</div>"


# ============================================================
# 完整 HTML 报告
# ============================================================
def generate_html(all_nd: list, all_metrics: dict, all_yesterday: dict,
                  all_history: dict, report_time: str) -> str:
    all_events = [e for nd in all_nd for e in nd.get("events", [])]

    # 导航栏
    nav_items = [
        ("#sec-summary", "执行摘要"),
    ]
    for nd in all_nd:
        node_id = nd["node_name"].replace("节点", "node")
        nav_items.append((f"#sec-{node_id}", nd["node_name"]))
    nav_items += [
        ("#sec-problems", "开放问题"),
        ("#sec-sp",       "ILOM信息"),
        ("#sec-trend",    "历史趋势"),
        ("#sec-events",   "事件日志"),
    ]
    nav_html = "".join(f'<a href="{href}">{lbl}</a>' for href, lbl in nav_items)

    # 节点详情
    node_secs = ""
    for nd in all_nd:
        node_id = nd["node_name"].replace("节点", "node")
        node_secs += f"<div id='sec-{node_id}'>" + _sec_node(nd) + "</div>"

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ODA X11 HA 巡检报告 {report_time}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>{CSS}</style>
</head>
<body>
<div class="hdr">
  <h1>Oracle ODA X11 HA 硬件巡检报告</h1>
  <p>设备: Oracle Database Appliance X11 High Availability &nbsp;|&nbsp;
     节点1: 10.1.250.101 &nbsp;|&nbsp; 节点2: 10.1.250.102 &nbsp;|&nbsp;
     巡检时间: {report_time}</p>
</div>
<nav>{nav_html}</nav>
<div class="wrap">

<div id="sec-summary">
{_summary_cards(all_nd, all_metrics, all_yesterday)}
{_sec_conclusion(all_nd, all_events)}
</div>

{node_secs}

{_sec_problems(all_nd)}
{_sec_sp(all_nd)}
{_sec_trends(all_history)}
{_sec_events(all_nd)}

<div class="foot">Oracle ODA X11 HA 巡检报告 · 生成时间 {report_time} · 保留30天</div>
</div>
</body>
</html>"""


# ============================================================
# 企业微信
# ============================================================
def send_wecom_markdown(content: str) -> None:
    resp = requests.post(SEND_URL, json={"msgtype":"markdown","markdown":{"content":content}}, timeout=15)
    resp.raise_for_status()
    r = resp.json()
    if r.get("errcode", 0) != 0: raise RuntimeError(f"企业微信错误: {r}")
    logger.info("企业微信 Markdown 发送成功")

def send_wecom_file(file_path: str, filename: str) -> None:
    last: Optional[Exception] = None
    for attempt in range(1, WECOM_RETRY_COUNT + 1):
        try:
            with open(file_path, "rb") as fh:
                resp = requests.post(UPLOAD_URL, files={"media":(filename,fh,"text/html")}, timeout=30)
            resp.raise_for_status()
            up = resp.json()
            if up.get("errcode", 0) != 0: raise RuntimeError(f"上传失败: {up}")
            mid = up.get("media_id")
            if not mid: raise RuntimeError(f"未获取 media_id: {up}")
            resp2 = requests.post(SEND_URL, json={"msgtype":"file","file":{"media_id":mid}}, timeout=15)
            resp2.raise_for_status()
            r2 = resp2.json()
            if r2.get("errcode", 0) != 0: raise RuntimeError(f"发送失败: {r2}")
            logger.info("企业微信文件发送成功 (attempt %d)", attempt); return
        except Exception as e:
            last = e; logger.warning("文件发送第%d次失败: %s", attempt, e)
            if attempt < WECOM_RETRY_COUNT: time.sleep(WECOM_RETRY_INTERVAL)
    logger.error("文件发送最终失败: %s", last)

def build_markdown(all_nd: list, all_metrics: dict, all_yesterday: dict, report_time: str) -> str:
    all_events = [e for nd in all_nd for e in nd.get("events", [])]
    major_cnt  = sum(1 for e in all_events if e.get("severity") in ("major","critical"))
    minor_cnt  = sum(1 for e in all_events if e.get("severity") == "minor")
    open_cnt   = sum(len(nd.get("open_problems",[])) for nd in all_nd)
    node_lines = []; inlets=[]; exhausts=[]; powers=[]; fans=[]; fan_maxes=[]
    for nd in all_nd:
        nm = nd["node_name"]; m = all_metrics.get(nm,{}); y = all_yesterday.get(nm) or {}
        h = _prop(nd.get("system",{}), "health", default="Unknown")
        tag = "✅正常" if h.lower()=="ok" else "⚠️告警"
        node_lines.append(f"**{nm}** {tag}")
        if m.get("inlet_temp")  is not None: inlets.append(m["inlet_temp"])
        if m.get("exhaust_temp") is not None: exhausts.append(m["exhaust_temp"])
        if m.get("actual_power_consumption") is not None: powers.append((nm, m["actual_power_consumption"]))
        if m.get("fan_percentage_avg") is not None:  fans.append(m["fan_percentage_avg"])
        for f in nd.get("cooling",{}).get("fans",[]):
            p = f.get("fan_percentage_num")
            if p is not None: fan_maxes.append(p)
    first_nm = all_nd[0]["node_name"] if all_nd else ""
    yest_in  = (all_yesterday.get(first_nm) or {}).get("inlet_temp")
    inlet_str  = (f"{inlets[0]:.0f}°C" + (f" ({diff_label(inlets[0],yest_in)})" if diff_label(inlets[0],yest_in) else "")) if inlets else "N/A"
    exhaust_str= f"{exhausts[0]:.0f}°C" if exhausts else "N/A"
    power_str  = " | ".join(f"{n}: {p:.0f}W" for n,p in powers) if powers else "N/A"
    fan_str    = f"{sum(fans)/len(fans):.0f}%" if fans else "N/A"
    fan_max    = f"{max(fan_maxes):.0f}%" if fan_maxes else "N/A"
    return (
        "## ODA X11 HA 硬件巡检报告\n"
        f"> 巡检时间：{report_time}\n\n"
        + "\n".join(node_lines) + "\n\n"
        f"**温度** 入口: {inlet_str} | 排风: {exhaust_str}\n"
        f"**功耗** {power_str}\n"
        f"**风扇** 平均: {fan_str} | 最高: {fan_max}\n"
        f"**事件** major: {major_cnt} | minor: {minor_cnt} | 开放问题: {open_cnt}\n\n"
        "> 详细报告见附件"
    )


# ============================================================
# 旧报告清理
# ============================================================
def cleanup_old_reports() -> None:
    cutoff = datetime.datetime.now() - datetime.timedelta(days=REPORT_RETAIN_DAYS)
    for fp in glob.glob(str(SCRIPT_DIR / "oda_report_*.html")):
        try:
            if datetime.datetime.fromtimestamp(os.path.getmtime(fp)) < cutoff:
                os.remove(fp); logger.info("已清理: %s", fp)
        except Exception as e:
            logger.warning("清理失败 %s: %s", fp, e)


# ============================================================
# 主流程
# ============================================================
def main(send: bool = True) -> None:
    now         = datetime.datetime.now()
    report_time = now.strftime("%Y-%m-%d %H:%M:%S")
    date_str    = now.strftime("%Y-%m-%d")
    fname       = f"oda_report_{now.strftime('%Y%m%d_%H%M%S')}.html"
    fpath       = SCRIPT_DIR / fname

    logger.info("=" * 60)
    logger.info("ODA 巡检开始: %s", report_time)
    init_db()

    all_nd: List[dict] = []
    for cfg in NODES:
        logger.info("采集 %s (%s)", cfg["name"], cfg["host"])
        all_nd.append(collect_node(cfg))

    all_metrics   = {}; all_yesterday = {}; all_history = {}
    for nd in all_nd:
        nm = nd["node_name"]
        m  = compute_metrics(nd)
        all_metrics[nm]   = m
        all_yesterday[nm] = get_yesterday(nm)
        save_metrics(date_str, nm, m)
        all_history[nm]   = get_history(nm, days=7)

    html = generate_html(all_nd, all_metrics, all_yesterday, all_history, report_time)
    fpath.write_text(html, encoding="utf-8")
    logger.info("报告已生成: %s", fpath)
    cleanup_old_reports()

    md = build_markdown(all_nd, all_metrics, all_yesterday, report_time)
    if send:
        try:    send_wecom_markdown(md)
        except Exception as e: logger.error("Markdown 推送失败: %s", e)
        try:    send_wecom_file(str(fpath), fname)
        except Exception as e: logger.error("文件推送失败: %s", e)
    else:
        logger.info("跳过企业微信推送 (--no-send)")

    logger.info("ODA 巡检完成: %s", report_time)
    logger.info("=" * 60)


if __name__ == "__main__":
    import sys
    main(send=("--no-send" not in sys.argv))


# ============================================================
# Windows 计划任务配置
# ============================================================
# 管理员 CMD 执行：
#
# schtasks /create /tn "ODA巡检" ^
#   /tr "\"C:\Python311\python.exe\" \"C:\scripts\oda_check.py\"" ^
#   /sc daily /st 08:00 /ru SYSTEM /f
#
# 查看：  schtasks /query /tn "ODA巡检" /fo LIST
# 测试：  schtasks /run /tn "ODA巡检"
# 删除：  schtasks /delete /tn "ODA巡检" /f
