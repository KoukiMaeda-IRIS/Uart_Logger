# -*- coding: utf-8 -*-
"""
ログ解析ビューア v9

【構成】
- 設定モデル: AppConfig / ByteRule
- 変換/評価: evaluate_byte_values, map関連
- I/O: SerialReader, TxtFileReader
- UI: ConfigDialog, MainWindow
"""

import os
import sys
import re
import json
import threading
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Tuple, Dict, Union
from collections import deque

from PySide6 import QtCore, QtWidgets, QtGui
import pyqtgraph as pg
import numpy as np

# UART（最小）
try:
    import serial
    import serial.tools.list_ports as list_ports
except Exception:
    serial = None
    list_ports = None

# =========================
# 定数/変換
# =========================

METHOD_CHOICES = [
    ("raw_u8",   "そのまま（0〜255）"),
    ("signed_u8","符号あり1バイト（-128〜127）"),
    ("map",      "値の置き換え（マップ）"),
    ("bitfield", "ビットフィールド（各ビット判定）"),
]
METHOD_LABEL_TO_ID = {label: mid for (mid, label) in METHOD_CHOICES}
METHOD_ID_TO_LABEL = {mid: label for (mid, label) in METHOD_CHOICES}

HEX_TOKEN = re.compile(r"(?:0x)?([0-9A-Fa-f]{2})")

DISPLAY_UNIT_CHOICES = [("ms", "ミリ秒"), ("s", "秒"), ("m", "分")]

# グループ未所属の内部キー
NO_GROUP_KEY = "__no_group__"
NO_GROUP_LABEL = "グループなし"

def to_seconds(value: float, unit: str) -> float:
    unit = unit.lower()
    if unit == "ms":
        return value / 1000.0
    if unit == "m":
        return value * 60.0
    return value

def display_scale_for(unit: str) -> float:
    unit = unit.lower()
    if unit == "ms":
        return 1000.0
    if unit == "m":
        return 1.0 / 60.0
    return 1.0

def display_unit_label(unit: str) -> str:
    return {"ms": "ms", "s": "s", "m": "min"}.get(unit.lower(), "s")

def s8(b: int) -> int:
    return b - 256 if b >= 128 else b

# =========================
# 設定モデル
# =========================

@dataclass
class ByteRule:
    enabled: bool = True
    name: str = ""
    method_id: str = "raw_u8"
    map_expr: str = ""          # 廃止予定（互換用に残す）
    map_label_expr: str = ""    # ラベルマップ: "0:停止,1:運転,2:異常,else:不明"
    graph_enabled: bool = False
    graph_label: str = ""
    graph_unit: str = ""
    graph_group: int = 0
    graph_group_name: str = ""
    bit_labels: List[str] = field(default_factory=lambda: ["", "", "", "", "", "", "", ""])

@dataclass
class AppConfig:
    history_seconds: float = 120.0
    show_hex_dump: bool = False

    sample_interval_value: float = 1.0
    sample_interval_unit: str = "s"
    x_display_unit: str = "s"

    port: str = ""
    baudrate: int = 9600

    frame_size: int = 4

    graph_group_names: List[str] = field(default_factory=lambda: ["グラフ1", "グラフ2"])

    byte_rules: List[ByteRule] = field(default_factory=lambda: [
        ByteRule(enabled=False, name="ヘッダ",    method_id="raw_u8",    graph_enabled=False),
        ByteRule(enabled=True,  name="温度",      method_id="signed_u8", graph_enabled=True, graph_unit="℃"),
        ByteRule(enabled=True,  name="モード",    method_id="map",       graph_enabled=True, map_label_expr="0:弱,1:中,2:強"),
        ByteRule(enabled=True,  name="カウンター", method_id="raw_u8",    graph_enabled=True),
    ])

    def save(self, path: str):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(self), f, ensure_ascii=False, indent=2)

    @staticmethod
    def load(path: str) -> "AppConfig":
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        cfg = AppConfig()
        valid_app_keys = {f.name for f in cfg.__dataclass_fields__.values()}
        valid_br_keys = {f.name for f in ByteRule.__dataclass_fields__.values()}
        for k, v in raw.items():
            if k == "byte_rules":
                items = []
                for br in v:
                    if br.get("method_id") in ("s16_be", "ignore"):
                        br["method_id"] = "raw_u8"
                    if "graph_group_name" not in br and "graph_group" in br:
                        br["graph_group_name"] = f"グラフ{br['graph_group']}" if br.get("graph_enabled") else ""
                    # bit_labelsが無い場合のデフォルト補完
                    if "bit_labels" not in br:
                        br["bit_labels"] = ["", "", "", "", "", "", "", ""]
                    elif len(br["bit_labels"]) < 8:
                        br["bit_labels"] += [""] * (8 - len(br["bit_labels"]))
                    br = {k2: v2 for k2, v2 in br.items() if k2 in valid_br_keys}
                    items.append(ByteRule(**br))
                cfg.byte_rules = items
            elif k in valid_app_keys:
                setattr(cfg, k, v)

        if len(cfg.byte_rules) != cfg.frame_size:
            rules = list(cfg.byte_rules)
            if len(rules) < cfg.frame_size:
                for i in range(len(rules), cfg.frame_size):
                    rules.append(ByteRule(enabled=True, name=f"データ{i+1}", method_id="raw_u8", graph_enabled=False))
            else:
                rules = rules[:cfg.frame_size]
            cfg.byte_rules = rules

        if not cfg.graph_group_names:
            names = []
            for r in cfg.byte_rules:
                if r.graph_group_name and r.graph_group_name not in names:
                    names.append(r.graph_group_name)
            cfg.graph_group_names = names if names else ["グラフ1"]

        cfg.sample_interval_unit = cfg.sample_interval_unit.lower()
        cfg.x_display_unit = cfg.x_display_unit.lower()
        return cfg

# =========================
# 変換ユーティリティ
# =========================

def parse_hex_line_to_bytes(line: str) -> Optional[bytes]:
    tokens = HEX_TOKEN.findall(line)
    if not tokens:
        return None
    try:
        return bytes(int(t, 16) for t in tokens)
    except Exception:
        return None

def parse_label_map_expr(expr: str) -> Tuple[Dict[int, str], Optional[str]]:
    """ラベルマップをパース: "0:停止,1:運転,else:不明" -> ({0:"停止", 1:"運転"}, "不明")"""
    table: Dict[int, str] = {}
    default_label: Optional[str] = None
    if not expr:
        return table, default_label
    for token in expr.split(","):
        token = token.strip()
        if not token or ":" not in token:
            continue
        k, v = token.split(":", 1)
        k = k.strip(); v = v.strip()
        if k.lower() in ("else", "default", "*"):
            default_label = v if v else None
            continue
        try:
            table[int(k, 0)] = v
        except:
            pass
    return table, default_label

def build_label_map_expr_from_table(pairs: List[Tuple[int, str]], default_label: Optional[str]) -> str:
    parts = [f"{int(k)}:{v}" for k, v in pairs]
    if default_label is not None:
        parts.append(f"else:{default_label}")
    return ",".join(parts)

# =========================
# 評価
# =========================

def evaluate_byte_values(frame: bytes, cfg: AppConfig) -> Dict[str, float]:
    """各バイトの数値を返す（グラフ用）。マップの場合は生値をそのまま使う."""
    out: Dict[str, float] = {}
    rules = cfg.byte_rules
    for i, raw_byte in enumerate(frame):
        if i >= cfg.frame_size:
            break
        rule = rules[i] if i < len(rules) else ByteRule()
        if not rule.enabled or rule.method_id == "ignore":
            continue
        name = rule.name.strip() or f"byte{i+1}"
        mid = rule.method_id
        val = float("nan")
        try:
            if mid == "raw_u8":
                val = float(raw_byte)
            elif mid == "signed_u8":
                val = float(s8(raw_byte))
            elif mid == "map":
                val = float(raw_byte)
            elif mid == "bitfield":
                val = float(raw_byte)
                # 各ビットを個別チャネルとして出力
                for bit_idx in range(8):
                    lbl = rule.bit_labels[bit_idx] if bit_idx < len(rule.bit_labels) else ""
                    if lbl:
                        bit_name = f"{name}_bit{bit_idx}_{lbl}"
                        out[bit_name] = float((raw_byte >> bit_idx) & 1)
        except:
            pass
        out[name] = val
    return out

def evaluate_byte_labels(frame: bytes, cfg: AppConfig) -> Dict[str, str]:
    """各バイトの表示用ラベル文字列を返す。マップの場合はラベルマップを使う."""
    out: Dict[str, str] = {}
    rules = cfg.byte_rules
    for i, raw_byte in enumerate(frame):
        if i >= cfg.frame_size:
            break
        rule = rules[i] if i < len(rules) else ByteRule()
        if not rule.enabled or rule.method_id == "ignore":
            continue
        name = rule.name.strip() or f"byte{i+1}"
        mid = rule.method_id
        label_str = ""
        try:
            if mid == "raw_u8":
                label_str = str(raw_byte)
            elif mid == "signed_u8":
                label_str = str(s8(raw_byte))
            elif mid == "map":
                ltable, ld = parse_label_map_expr(rule.map_label_expr)
                if ltable or ld is not None:
                    lbl = ltable.get(raw_byte, ld if ld is not None else "?")
                    label_str = f"{lbl}(raw:{raw_byte})"
                else:
                    label_str = str(raw_byte)
            elif mid == "bitfield":
                active_labels = []
                for bit_idx in range(8):
                    if (raw_byte >> bit_idx) & 1:
                        bit_lbl = rule.bit_labels[bit_idx] if bit_idx < len(rule.bit_labels) else ""
                        if bit_lbl:
                            active_labels.append(bit_lbl)
                        else:
                            active_labels.append(f"bit{bit_idx}")
                if active_labels:
                    label_str = f"{'+'.join(active_labels)}(0x{raw_byte:02X})"
                else:
                    label_str = f"なし(0x{raw_byte:02X})"
        except:
            pass
        out[name] = label_str
    return out

def build_y_tick_labels(rule: ByteRule) -> Optional[List[Tuple[float, str]]]:
    """マップルールからY軸ティックラベル一覧を構築。生値→ラベルの対応を返す."""
    if rule.method_id == "map":
        ltable, ld = parse_label_map_expr(rule.map_label_expr)
        if not ltable and ld is None:
            return None
        ticks: List[Tuple[float, str]] = []
        for raw_key, lbl in sorted(ltable.items()):
            ticks.append((float(raw_key), lbl))
        return ticks if ticks else None
    elif rule.method_id == "bitfield":
        # ビットフィールドはサブチャネル(0/1)なのでティックは不要
        return None
    return None

# =========================
# データバッファ
# =========================

class RingBuffer:
    def __init__(self, maxlen: int = 1_000_000):
        self.t = deque(maxlen=maxlen)
        self.series: Dict[str, deque] = {}

    def ensure(self, name: str):
        if name not in self.series:
            self.series[name] = deque(maxlen=self.t.maxlen)

    def ensure_many(self, names: List[str]):
        for szName in names:
            self.ensure(szName)

    def append(self, index: int, values: Dict[str, float], active_names: List[str]):
        # 全系列を同じ長さで保持する（無い値は NaN）
        self.t.append(index)
        self.ensure_many(active_names)
        for szName in active_names:
            if szName in values:
                self.series[szName].append(values[szName])
            else:
                self.series[szName].append(float("nan"))

# =========================
# I/O スレッド
# =========================

class SerialReader(QtCore.QThread):
    new_frame = QtCore.Signal(bytes)
    def __init__(self, cfg: AppConfig, parent=None):
        super().__init__(parent); self.cfg = cfg
        self._stop = threading.Event(); self._ser = None; self._buf = bytearray()
    def stop(self): self._stop.set()
    def run(self):
        if serial is None: return
        try: self._ser = serial.Serial(port=self.cfg.port, baudrate=self.cfg.baudrate, timeout=0.05)
        except Exception as e: print("Serial open error:", e); return
        fs = self.cfg.frame_size
        try:
            while not self._stop.is_set():
                n = self._ser.in_waiting or 1
                chunk = self._ser.read(n)
                if not chunk: continue
                self._buf.extend(chunk)
                while len(self._buf) >= fs:
                    frame = bytes(self._buf[:fs]); del self._buf[:fs]
                    self.new_frame.emit(frame)
        finally:
            try:
                if self._ser and self._ser.is_open: self._ser.close()
            except: pass

class TxtFileReader(QtCore.QThread):
    new_frame = QtCore.Signal(bytes, int)
    finished = QtCore.Signal()
    def __init__(self, path: str, cfg: AppConfig, parent=None):
        super().__init__(parent); self.path = path; self.cfg = cfg
    def run(self):
        fs = self.cfg.frame_size; buf = bytearray(); idx = 0
        try:
            with open(self.path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    b = parse_hex_line_to_bytes(line)
                    if not b: continue
                    buf.extend(b)
                    while len(buf) >= fs:
                        frame = bytes(buf[:fs]); del buf[:fs]
                        self.new_frame.emit(frame, idx); idx += 1
        finally:
            self.finished.emit()

# =========================
# プロット
# =========================

class DraggablePlotContainer(QtWidgets.QWidget):
    def __init__(self, gname: str, plot_group: "ChannelPlotGroup"):
        super().__init__()
        self.gname = gname
        self.plot_group = plot_group
        lay = QtWidgets.QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        
        self.handle = QtWidgets.QLabel("↕")
        self.handle.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop | QtCore.Qt.AlignmentFlag.AlignHCenter)
        self.handle.setCursor(QtCore.Qt.CursorShape.OpenHandCursor)
        self.handle.setStyleSheet("font-size:24px; padding-top:10px; color:#555;")
        self.handle.setFixedWidth(40)
        
        lay.addWidget(self.handle)
        lay.addWidget(self.plot_group.widget, 1)

    def mousePressEvent(self, event: QtGui.QMouseEvent):
        pos = event.position().toPoint()
        if event.button() == QtCore.Qt.MouseButton.LeftButton and self.handle.geometry().contains(pos):
            self.drag_start_pos = pos
        else:
            super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QtGui.QMouseEvent):
        if not (event.buttons() & QtCore.Qt.MouseButton.LeftButton): return
        if getattr(self, "drag_start_pos", None) is None: return
        
        pos = event.position().toPoint()
        if (pos - self.drag_start_pos).manhattanLength() < QtWidgets.QApplication.startDragDistance(): return
        
        drag = QtGui.QDrag(self)
        mime_data = QtCore.QMimeData()
        mime_data.setText(self.gname)
        drag.setMimeData(mime_data)
        
        pixmap = self.grab()
        drag.setPixmap(pixmap)
        drag.setHotSpot(pos)
        
        drag.exec(QtCore.Qt.DropAction.MoveAction)
        self.drag_start_pos = None

class PlotListContainer(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.main_layout = QtWidgets.QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        
    def dragEnterEvent(self, event: QtGui.QDragEnterEvent):
        if event.mimeData().hasText(): event.acceptProposedAction()
            
    def dragMoveEvent(self, event: QtGui.QDragMoveEvent):
        event.acceptProposedAction()
        
    def dropEvent(self, event: QtGui.QDropEvent):
        source_widget = event.source()
        if not isinstance(source_widget, DraggablePlotContainer): return
        
        drop_y = event.position().y()
        insert_idx = self.main_layout.count() - 1
        for i in range(self.main_layout.count() - 1):
            item = self.main_layout.itemAt(i)
            w = item.widget()
            if w and drop_y < w.y() + w.height() / 2:
                insert_idx = i
                break
                
        self.main_layout.removeWidget(source_widget)
        self.main_layout.insertWidget(insert_idx, source_widget)
        event.acceptProposedAction()

class ChannelPlotGroup:
    def __init__(self, title: str, unit: str, xlink=None, min_height: int = 250):
        self.title = title; self.unit = unit
        self.widget = pg.PlotWidget()
        self.widget.setMinimumHeight(min_height)
        self.widget.setBackground("w"); self.widget.showGrid(x=True, y=True, alpha=0.3)
        self.widget.setMouseEnabled(x=True, y=True); self.widget.setMenuEnabled(False)
        self.widget.addLegend()
        self.widget.getAxis("bottom").enableAutoSIPrefix(False)
        self.widget.getAxis("left").enableAutoSIPrefix(False)
        self.widget.setLabel("left", f"{title} ({unit})" if unit else title, **{"color":"#444"})
        self.widget.setLabel("bottom", "時間", **{"color":"#444"})
        if xlink is not None: self.widget.setXLink(xlink)
        self.widget.scene().sigMouseClicked.connect(self._reset_on_right)
        self.curves: Dict[str, pg.PlotDataItem] = {}
        self._y_tick_labels: Optional[List[Tuple[float, str]]] = None

    def _reset_on_right(self, ev):
        if ev.button() == QtCore.Qt.MouseButton.RightButton:
            self.widget.enableAutoRange()

    def add_or_get_curve(self, src_name: str, label: str, color: QtGui.QColor):
        if src_name in self.curves: return self.curves[src_name]
        curve = self.widget.plot(name=label, pen=pg.mkPen(color, width=2))
        self.curves[src_name] = curve; return curve

    def update_axis_label(self, x_unit_str: str):
        self.widget.setLabel("bottom", f"時間 [{x_unit_str}]", **{"color":"#444"})

    def set_y_tick_labels(self, ticks: Optional[List[Tuple[float, str]]]):
        """Y軸にラベルティックを設定する。Noneなら通常の数値表示に戻す."""
        self._y_tick_labels = ticks
        left_axis = self.widget.getAxis("left")
        if ticks:
            left_axis.setTicks([ticks])
        else:
            left_axis.setTicks(None)

# =========================
# 設定ダイアログ
# =========================

class NoWheelComboBox(QtWidgets.QComboBox):
    def wheelEvent(self, e: QtGui.QWheelEvent) -> None:
        e.ignore()

class ConfigDialog(QtWidgets.QDialog):
    def __init__(self, cfg: AppConfig, parent=None):
        super().__init__(parent)
        self.setWindowTitle("設定"); self.setMinimumWidth(1020)
        self.cfg = cfg

        tabs = QtWidgets.QTabWidget(self)

        # --- 基本設定 ---
        base = QtWidgets.QWidget(); f = QtWidgets.QFormLayout(base)
        self.spin_frame_size = QtWidgets.QSpinBox(); self.spin_frame_size.setRange(1, 4096); self.spin_frame_size.setValue(int(cfg.frame_size))
        self.spin_interval = QtWidgets.QDoubleSpinBox(); self.spin_interval.setRange(0.001, 3_600_000.0); self.spin_interval.setDecimals(3); self.spin_interval.setValue(cfg.sample_interval_value)
        self.cmb_interval_unit = QtWidgets.QComboBox(); self.cmb_interval_unit.addItems(["ms", "s", "m"]); self.cmb_interval_unit.setCurrentText(cfg.sample_interval_unit)
        h1 = QtWidgets.QHBoxLayout(); h1.addWidget(self.spin_interval); h1.addWidget(self.cmb_interval_unit); h1.addStretch()
        self.cmb_xunit = QtWidgets.QComboBox(); self.cmb_xunit.addItems(["ms", "s", "m"]); self.cmb_xunit.setCurrentText(cfg.x_display_unit)
        f.addRow("処理する総バイト数", self.spin_frame_size)
        f.addRow("データ間隔", self._wrap(h1))
        f.addRow("X軸 表示単位", self.cmb_xunit)
        tabs.addTab(base, "基本設定")

        # --- バイト設定 ---
        self.byte_tabs = QtWidgets.QTabWidget(); self.byte_editors = []
        tabs.addTab(self.byte_tabs, "バイト設定")

        # --- グラフ設定 ---
        graph = QtWidgets.QWidget(); self._graph_layout = QtWidgets.QVBoxLayout(graph)
        self.tbl_graph = None; self.graph_editors = []
        self._group_name_list = list(cfg.graph_group_names) if cfg.graph_group_names else ["グラフ1"]
        tabs.addTab(graph, "グラフ設定")

        # 初期構築
        self._rebuild_byte_tabs(int(cfg.frame_size))
        self._rebuild_graph_table(int(cfg.frame_size))
        self.spin_frame_size.valueChanged.connect(self._on_frame_size_changed)

        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        layout = QtWidgets.QVBoxLayout(self); layout.addWidget(tabs); layout.addWidget(btns)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject)

    def _on_frame_size_changed(self, n: int):
        self._rebuild_byte_tabs(int(n)); self._rebuild_graph_table(int(n))

    def _rebuild_byte_tabs(self, n: int):
        while self.byte_tabs.count() > 0: self.byte_tabs.removeTab(0)
        self.byte_editors = []
        lstRules = list(self.cfg.byte_rules)
        if len(lstRules) < n: lstRules += [ByteRule() for _ in range(n - len(lstRules))]
        else: lstRules = lstRules[:n]

        for i in range(n):
            stRule = lstRules[i]
            if not stRule.name or not stRule.name.strip(): stRule.name = f"データ{i+1}"

            wTab = QtWidgets.QWidget(); f = QtWidgets.QFormLayout(wTab)
            chk_enabled = QtWidgets.QCheckBox("有効"); chk_enabled.setChecked(bool(stRule.enabled))
            edt_name = QtWidgets.QLineEdit(stRule.name)
            cmb_method = NoWheelComboBox()
            cmb_method.addItems([label for _, label in METHOD_CHOICES])
            cmb_method.setCurrentText(METHOD_ID_TO_LABEL.get(stRule.method_id, "使わない"))

            # バイト名変更 → グラフ設定タブの対象名(0列)に反映
            def _sync_graph_target(_text, idx=i, edit=edt_name):
                if self.tbl_graph is None: return
                item = self.tbl_graph.item(idx, 0)
                if item is None: return
                item.setText(edit.text().strip() or f"データ{idx+1}")
            edt_name.textChanged.connect(_sync_graph_target)

            # マップページ（ラベルのみ: 受信値, 表示ラベル）
            map_page = QtWidgets.QWidget(); map_v = QtWidgets.QVBoxLayout(map_page); map_v.setContentsMargins(0,0,0,0)

            tbl_map = QtWidgets.QTableWidget(0, 2)
            tbl_map.setHorizontalHeaderLabels(["受信値(0-255)", "表示ラベル"])
            tbl_map.horizontalHeader().setStretchLastSection(True); tbl_map.verticalHeader().setVisible(False)
            btn_add = QtWidgets.QPushButton("行追加"); btn_del = QtWidgets.QPushButton("行削除")
            hbtn = QtWidgets.QHBoxLayout(); hbtn.addWidget(btn_add); hbtn.addWidget(btn_del); hbtn.addStretch()

            edt_else_label = QtWidgets.QLineEdit(); edt_else_label.setPlaceholderText("ラベル（空=表示なし）")
            form_else = QtWidgets.QFormLayout()
            form_else.addRow("その他(else) ラベル", edt_else_label)

            map_v.addWidget(tbl_map); map_v.addLayout(hbtn); map_v.addLayout(form_else)

            # 既存データを2列テーブルに復元
            dctLabel, fDefaultLabel = parse_label_map_expr(stRule.map_label_expr)
            for ucKey in sorted(dctLabel.keys()):
                r = tbl_map.rowCount(); tbl_map.insertRow(r)
                tbl_map.setItem(r, 0, QtWidgets.QTableWidgetItem(str(int(ucKey))))
                tbl_map.setItem(r, 1, QtWidgets.QTableWidgetItem(dctLabel.get(ucKey, "")))
            if fDefaultLabel is not None: edt_else_label.setText(fDefaultLabel)

            def _add_row(_c=False, tbl=tbl_map):
                r = tbl.rowCount(); tbl.insertRow(r)
                tbl.setItem(r, 0, QtWidgets.QTableWidgetItem(""))
                tbl.setItem(r, 1, QtWidgets.QTableWidgetItem(""))
            def _del_row(_c=False, tbl=tbl_map):
                for rr in sorted({idx.row() for idx in tbl.selectedIndexes()}, reverse=True): tbl.removeRow(rr)
            btn_add.clicked.connect(_add_row); btn_del.clicked.connect(_del_row)

            # ビットフィールドページ
            bitfield_page = QtWidgets.QWidget()
            bitfield_v = QtWidgets.QVBoxLayout(bitfield_page)
            bitfield_v.setContentsMargins(0, 0, 0, 0)

            bitfield_info = QtWidgets.QLabel(
                "各ビットが1の時に表示するラベルを設定します。\n"
                "例: 0x12(=0b00010010) → bit1とbit4が立っている → 設定したラベルを表示"
            )
            bitfield_info.setStyleSheet("color:#555; font-size:11px; padding:4px;")
            bitfield_v.addWidget(bitfield_info)

            tbl_bits = QtWidgets.QTableWidget(8, 3)
            tbl_bits.setHorizontalHeaderLabels(["ビット", "HEXマスク", "1の時のラベル"])
            tbl_bits.horizontalHeader().setStretchLastSection(True)
            tbl_bits.verticalHeader().setVisible(False)
            tbl_bits.setColumnWidth(0, 60)
            tbl_bits.setColumnWidth(1, 80)

            bit_label_edits = []
            for bit_idx in range(8):
                # ビット番号(読み取り専用)
                item_bit = QtWidgets.QTableWidgetItem(f"bit{bit_idx}")
                item_bit.setFlags(item_bit.flags() & ~QtCore.Qt.ItemFlag.ItemIsEditable)
                tbl_bits.setItem(bit_idx, 0, item_bit)

                # HEXマスク(読み取り専用)
                item_hex = QtWidgets.QTableWidgetItem(f"0x{(1 << bit_idx):02X}")
                item_hex.setFlags(item_hex.flags() & ~QtCore.Qt.ItemFlag.ItemIsEditable)
                tbl_bits.setItem(bit_idx, 1, item_hex)

                # ラベル入力
                existing_label = ""
                if bit_idx < len(stRule.bit_labels):
                    existing_label = stRule.bit_labels[bit_idx]
                edt_bit = QtWidgets.QLineEdit(existing_label)
                edt_bit.setPlaceholderText(f"bit{bit_idx}のラベル")
                tbl_bits.setCellWidget(bit_idx, 2, edt_bit)
                bit_label_edits.append(edt_bit)

            bitfield_v.addWidget(tbl_bits)

            empty_page = QtWidgets.QLabel("（この方法では詳細設定はありません）")
            empty_page.setStyleSheet("color:#666; padding:6px;")
            stack = QtWidgets.QStackedWidget()
            stack.addWidget(empty_page)       # index 0: その他
            stack.addWidget(map_page)          # index 1: マップ
            stack.addWidget(bitfield_page)     # index 2: ビットフィールド

            def _on_method_changed(_text, cmb=cmb_method, st=stack, tbl=tbl_map):
                mid = METHOD_LABEL_TO_ID.get(cmb.currentText(), "raw_u8")
                if mid == "map":
                    st.setCurrentIndex(1)
                    if tbl.rowCount() == 0:
                        for rx in ["0","1","2"]:
                            rr = tbl.rowCount(); tbl.insertRow(rr)
                            tbl.setItem(rr, 0, QtWidgets.QTableWidgetItem(rx))
                            tbl.setItem(rr, 1, QtWidgets.QTableWidgetItem(""))
                elif mid == "bitfield":
                    st.setCurrentIndex(2)
                else:
                    st.setCurrentIndex(0)
            cmb_method.currentTextChanged.connect(_on_method_changed); _on_method_changed(cmb_method.currentText())

            f.addRow("", chk_enabled); f.addRow("名前", edt_name); f.addRow("方法", cmb_method); f.addRow("詳細設定", stack)
            self.byte_editors.append({
                "chk_enabled": chk_enabled, "edt_name": edt_name, "cmb_method": cmb_method,
                "tbl_map": tbl_map, "edt_else_label": edt_else_label,
                "bit_label_edits": bit_label_edits
            })
            self.byte_tabs.addTab(wTab, f"Byte{i+1}")

    def _rebuild_graph_table(self, n: int):
        while self._graph_layout.count() > 0:
            item = self._graph_layout.takeAt(0); w = item.widget()
            if w is not None: w.setParent(None)

        rules = list(self.cfg.byte_rules)
        if len(rules) < n: rules += [ByteRule() for _ in range(n - len(rules))]
        else: rules = rules[:n]

        grp_bar = QtWidgets.QWidget()
        grp_h = QtWidgets.QHBoxLayout(grp_bar); grp_h.setContentsMargins(0,0,0,0)
        grp_h.addWidget(QtWidgets.QLabel("グループ:"))
        self.grp_list_widget = QtWidgets.QComboBox()
        for gn in self._group_name_list:
            self.grp_list_widget.addItem(gn)
        self.grp_list_widget.setMinimumWidth(120)
        grp_h.addWidget(self.grp_list_widget)

        btn_grp_add = QtWidgets.QPushButton("+"); btn_grp_add.setFixedWidth(30)
        btn_grp_del = QtWidgets.QPushButton("-"); btn_grp_del.setFixedWidth(30)
        btn_grp_rename = QtWidgets.QPushButton("名前変更")
        grp_h.addWidget(btn_grp_add); grp_h.addWidget(btn_grp_del); grp_h.addWidget(btn_grp_rename)
        grp_h.addStretch()

        def _add_grp():
            text, ok = QtWidgets.QInputDialog.getText(self, "グループ追加", "グループ名:")
            if ok and text.strip():
                name = text.strip()
                if name not in self._group_name_list:
                    self._group_name_list.append(name)
                    self.grp_list_widget.addItem(name)
                    self._update_group_combos()
        def _del_grp():
            idx = self.grp_list_widget.currentIndex()
            if idx < 0: return
            name = self.grp_list_widget.currentText()
            self._group_name_list.remove(name)
            self.grp_list_widget.removeItem(idx)
            self._update_group_combos()
        def _rename_grp():
            idx = self.grp_list_widget.currentIndex()
            if idx < 0: return
            old = self.grp_list_widget.currentText()
            text, ok = QtWidgets.QInputDialog.getText(self, "名前変更", "新しい名前:", text=old)
            if ok and text.strip():
                new = text.strip(); gi = self._group_name_list.index(old)
                self._group_name_list[gi] = new
                self.grp_list_widget.setItemText(idx, new)
                self._update_group_combos(old, new)

        btn_grp_add.clicked.connect(_add_grp); btn_grp_del.clicked.connect(_del_grp); btn_grp_rename.clicked.connect(_rename_grp)
        self._graph_layout.addWidget(grp_bar)

        self.graph_editors = []
        self.tbl_graph = QtWidgets.QTableWidget(n, 4)
        self.tbl_graph.setHorizontalHeaderLabels(["対象", "グラフ表示", "グループ", "単位"])
        self.tbl_graph.horizontalHeader().setStretchLastSection(True)
        self.tbl_graph.verticalHeader().setVisible(False)

        for i in range(n):
            rule = rules[i]
            name = (rule.name or "").strip() or f"データ{i+1}"

            item_target = QtWidgets.QTableWidgetItem(name)
            item_target.setFlags(item_target.flags() & ~QtCore.Qt.ItemFlag.ItemIsEditable)
            self.tbl_graph.setItem(i, 0, item_target)

            chk = QtWidgets.QTableWidgetItem("")
            chk.setFlags(chk.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
            chk.setCheckState(QtCore.Qt.CheckState.Checked if rule.graph_enabled else QtCore.Qt.CheckState.Unchecked)
            self.tbl_graph.setItem(i, 1, chk)

            cmb_grp = NoWheelComboBox()
            cmb_grp.addItem(NO_GROUP_LABEL)
            cmb_grp.addItems(self._group_name_list)
            if rule.graph_group_name and rule.graph_group_name in self._group_name_list:
                cmb_grp.setCurrentText(rule.graph_group_name)
            else:
                cmb_grp.setCurrentIndex(0)
            self.tbl_graph.setCellWidget(i, 2, cmb_grp)

            edt_unit = QtWidgets.QLineEdit(rule.graph_unit)
            self.tbl_graph.setCellWidget(i, 3, edt_unit)

            self.graph_editors.append({"cmb_grp": cmb_grp, "edt_unit": edt_unit})

        self._graph_layout.addWidget(self.tbl_graph)

    def _update_group_combos(self, old_name: str = None, new_name: str = None):
        for ed in self.graph_editors:
            cmb = ed["cmb_grp"]
            current = cmb.currentText()
            if old_name and current == old_name:
                current = new_name if new_name else NO_GROUP_LABEL
            cmb.blockSignals(True)
            cmb.clear(); cmb.addItem(NO_GROUP_LABEL); cmb.addItems(self._group_name_list)
            cmb.setCurrentText(current if current in self._group_name_list else NO_GROUP_LABEL)
            cmb.blockSignals(False)

    def get_config(self) -> AppConfig:
        cfg = AppConfig()
        cfg.frame_size = int(self.spin_frame_size.value())
        cfg.sample_interval_value = float(self.spin_interval.value())
        cfg.sample_interval_unit = self.cmb_interval_unit.currentText().lower()
        cfg.x_display_unit = self.cmb_xunit.currentText().lower()
        cfg.show_hex_dump = False
        cfg.graph_group_names = list(self._group_name_list)

        rules: List[ByteRule] = []
        for i in range(cfg.frame_size):
            ed = self.byte_editors[i]
            enabled = ed["chk_enabled"].isChecked()
            name = ed["edt_name"].text()
            method_id = METHOD_LABEL_TO_ID.get(ed["cmb_method"].currentText(), "raw_u8")

            map_label_expr = ""
            bit_labels = ["", "", "", "", "", "", "", ""]
            if method_id == "map":
                tbl = ed["tbl_map"]
                label_pairs: List[Tuple[int, str]] = []
                for r in range(tbl.rowCount()):
                    ik = tbl.item(r, 0); il = tbl.item(r, 1)
                    kt = ik.text().strip() if ik else ""
                    lt = il.text().strip() if il else ""
                    if not kt and not lt: continue
                    try: k = int(kt, 10)
                    except: continue
                    if k < 0 or k > 255:
                        QtWidgets.QMessageBox.warning(self, "マップ設定エラー", f"Byte{i+1}: 受信値が範囲外です: {k}（0〜255）")
                        raise ValueError(f"受信値が範囲外: {k}")
                    if lt:
                        label_pairs.append((k, lt))

                else_label = ed["edt_else_label"].text().strip() or None
                map_label_expr = build_label_map_expr_from_table(label_pairs, else_label)

            elif method_id == "bitfield":
                for bit_idx in range(8):
                    edt = ed["bit_label_edits"][bit_idx]
                    bit_labels[bit_idx] = edt.text().strip()

            rules.append(ByteRule(enabled=enabled, name=name, method_id=method_id,
                                  map_label_expr=map_label_expr, bit_labels=bit_labels))

        for i in range(cfg.frame_size):
            item_chk = self.tbl_graph.item(i, 1)
            graph_enabled = (item_chk.checkState() == QtCore.Qt.CheckState.Checked) if item_chk else False
            grp_name = self.graph_editors[i]["cmb_grp"].currentText()
            if grp_name == NO_GROUP_LABEL: grp_name = ""
            graph_unit = self.graph_editors[i]["edt_unit"].text()

            rules[i].graph_enabled = graph_enabled
            rules[i].graph_group_name = grp_name
            rules[i].graph_unit = graph_unit

        cfg.byte_rules = rules
        cfg.port = self.cfg.port; cfg.baudrate = self.cfg.baudrate
        return cfg

    def _wrap(self, layout_or_widget):
        w = QtWidgets.QWidget()
        if isinstance(layout_or_widget, QtWidgets.QLayout): w.setLayout(layout_or_widget)
        else: lay = QtWidgets.QHBoxLayout(w); lay.addWidget(layout_or_widget); lay.addStretch()
        return w

# =========================
# メインUI
# =========================

def available_ports() -> List[str]:
    if list_ports is None: return []
    return [p.device for p in list_ports.comports()]

SETTINGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "setting")
DEFAULT_CONFIG_NAME = "default.json"
PREFERRED_CONFIG_NAME = "test.json"


def _resolve_startup_config_path() -> str:
    if not os.path.isdir(SETTINGS_DIR):
        os.makedirs(SETTINGS_DIR, exist_ok=True)

    json_files = [
        fn for fn in os.listdir(SETTINGS_DIR)
        if fn.lower().endswith(".json") and os.path.isfile(os.path.join(SETTINGS_DIR, fn))
    ]

    # .json が無い時だけ default.json を返す
    if len(json_files) == 0:
        return os.path.join(SETTINGS_DIR, DEFAULT_CONFIG_NAME)

    # test.json 優先
    for fn in json_files:
        if fn.lower() == PREFERRED_CONFIG_NAME.lower():
            return os.path.join(SETTINGS_DIR, fn)

    json_files.sort()
    return os.path.join(SETTINGS_DIR, json_files[0])


AUTO_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ログ解析ビューア v9")
        self.resize(1420, 960)
        self.cfg = AppConfig(); self.buffer = RingBuffer(); self.current_time_sec = 0.0
        self.serial_thread = None; self.file_thread = None
        self.plot_groups: Dict[str, ChannelPlotGroup] = {}
        self._plot_min_height = 250
        self.crosshairs: Dict[str, pg.InfiniteLine] = {}
        self._mouse_scenes: List[QtWidgets.QGraphicsScene] = []
        self._current_cfg_path: str = _resolve_startup_config_path()
        self._label_tick_map: Dict[str, Dict[float, str]] = {}
        self._build_ui()
        self.update_timer = QtCore.QTimer(self); self.update_timer.setInterval(50)
        self.update_timer.timeout.connect(self._refresh_plots); self.update_timer.start()

    def _build_ui(self):
        central = QtWidgets.QWidget(); self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)

        # --- UART受信中インジケータ（上部） ---
        self.uart_indicator = QtWidgets.QLabel("● UART受信中")
        self.uart_indicator.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.uart_indicator.setStyleSheet(
            "background:#fff3cd;color:#856404;font-weight:bold;font-size:13px;"
            "padding:4px;border:1px solid #ffc107;border-radius:4px;")
        self.uart_indicator.setVisible(False)
        root.addWidget(self.uart_indicator)

        # 点滅タイマー
        self._uart_blink_timer = QtCore.QTimer(self)
        self._uart_blink_timer.setInterval(500)
        self._uart_blink_on = True
        self._uart_blink_timer.timeout.connect(self._blink_uart_indicator)

        # --- 設定保存通知（上部、控えめ） ---
        self.save_notify_label = QtWidgets.QLabel("")
        self.save_notify_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight)
        self.save_notify_label.setStyleSheet(
            "color:#d00;font-size:10px;font-weight:bold;padding:0px 8px 0px 0px;")
        root.addWidget(self.save_notify_label)
        self._save_notify_timer = QtCore.QTimer(self)
        self._save_notify_timer.setSingleShot(True)
        self._save_notify_timer.timeout.connect(lambda: self.save_notify_label.setText(""))

        # メイン横レイアウト
        hbox = QtWidgets.QHBoxLayout()
        root.addLayout(hbox, 1)

        panel = QtWidgets.QFrame(); panel.setFrameShape(QtWidgets.QFrame.StyledPanel); panel.setFixedWidth(380)
        pv = QtWidgets.QVBoxLayout(panel)

        grp_uart = QtWidgets.QGroupBox("UART"); gf = QtWidgets.QFormLayout(grp_uart)
        self.port_cb = QtWidgets.QComboBox(); self.port_cb.setEditable(True); self._reload_ports()
        self.baud_cb = QtWidgets.QComboBox(); self.baud_cb.addItems([str(b) for b in [9600,19200,38400,57600,115200,230400,460800,921600]])
        self.baud_cb.setCurrentText(str(self.cfg.baudrate))
        self.btn_connect = QtWidgets.QPushButton("接続"); self.btn_disconnect = QtWidgets.QPushButton("切断")
        row = QtWidgets.QHBoxLayout(); row.addWidget(self.btn_connect); row.addWidget(self.btn_disconnect)
        gf.addRow("ポート", self.port_cb); gf.addRow("ボーレート", self.baud_cb); gf.addRow(row)
        pv.addWidget(grp_uart)

        grp_file = QtWidgets.QGroupBox("ログファイル（.txt）"); fv = QtWidgets.QVBoxLayout(grp_file)
        self.btn_bin2hex = QtWidgets.QPushButton("opnelog用バイナリ→HEX変換"); fv.addWidget(self.btn_bin2hex)
        self.btn_open = QtWidgets.QPushButton("ログデータを開く(.txt)")
        self.btn_open.setStyleSheet("""
            QPushButton { background-color: #28a745; color: white; font-weight: bold; border-radius: 4px; padding: 6px; }
            QPushButton:hover { background-color: #218838; }
            QPushButton:pressed { background-color: #1e7e34; }
        """)
        fv.addWidget(self.btn_open)
        pv.addWidget(grp_file)

        grp_cfg = QtWidgets.QGroupBox("設定"); cv = QtWidgets.QHBoxLayout(grp_cfg)
        self.btn_open_cfg = QtWidgets.QPushButton("設定を開く")
        self.btn_save_cfg = QtWidgets.QPushButton("保存"); self.btn_load_cfg = QtWidgets.QPushButton("読込")
        cv.addWidget(self.btn_open_cfg); cv.addStretch(); cv.addWidget(self.btn_save_cfg); cv.addWidget(self.btn_load_cfg)
        pv.addWidget(grp_cfg)

        pv.addStretch()
        self.file_name_label = QtWidgets.QLabel("ファイル: -")
        self.file_name_label.setStyleSheet("color: #0056b3; font-weight: bold;")
        pv.addWidget(self.file_name_label)
        
        self.status_label = QtWidgets.QLabel("状態: 待機中"); pv.addWidget(self.status_label)
        hbox.addWidget(panel)

        right = QtWidgets.QVBoxLayout(); hbox.addLayout(right, 1)
        self.value_bar_scroll = QtWidgets.QScrollArea(); self.value_bar_scroll.setWidgetResizable(True)
        self.value_bar_container = QtWidgets.QWidget()
        self.value_bar_layout = QtWidgets.QHBoxLayout(self.value_bar_container)
        self.value_bar_layout.setContentsMargins(8,8,8,0); self.value_bar_layout.setSpacing(8); self.value_bar_layout.addStretch()
        self.value_bar_scroll.setWidget(self.value_bar_container); right.addWidget(self.value_bar_scroll)

        # カーソル読み取り ＋ グラフ高さ調整ボタン
        readout_layout = QtWidgets.QHBoxLayout()
        self.readout_label = QtWidgets.QLabel("カーソル: -")
        self.readout_label.setStyleSheet("font-family:Consolas,'Cascadia Code',monospace;padding:6px;background:#f5f5f5;border:1px solid #ddd;border-radius:6px;")
        readout_layout.addWidget(self.readout_label, 1)

        self.measure_label = QtWidgets.QLabel("測定: -")
        self.measure_label.setStyleSheet("font-family:Consolas,'Cascadia Code',monospace;padding:6px;background:#eef;border:1px solid #ccd;border-radius:6px;")
        readout_layout.addWidget(self.measure_label, 0)

        readout_layout.addWidget(QtWidgets.QLabel("グラフ高さ:"))
        self.btn_plot_shrink = QtWidgets.QPushButton("－"); self.btn_plot_shrink.setFixedWidth(30)
        self.btn_plot_expand = QtWidgets.QPushButton("＋"); self.btn_plot_expand.setFixedWidth(30)
        self.btn_plot_shrink.clicked.connect(self._shrink_plots)
        self.btn_plot_expand.clicked.connect(self._expand_plots)
        readout_layout.addWidget(self.btn_plot_shrink)
        readout_layout.addWidget(self.btn_plot_expand)

        right.addLayout(readout_layout)

        self.plot_scroll = QtWidgets.QScrollArea(); self.plot_scroll.setWidgetResizable(True)
        self.plot_container = PlotListContainer(); self.plot_layout = self.plot_container.main_layout
        self.plot_scroll.setWidget(self.plot_container); right.addWidget(self.plot_scroll, 1)

        self.value_cards: Dict[int, QtWidgets.QLabel] = {}
        self.unit_edits: Dict[int, QtWidgets.QLineEdit] = {}
        self.name_by_index: Dict[int, str] = {}
        self.graph_name_by_index: Dict[int, str] = {}
        self.graph_unit_by_index: Dict[int, str] = {}
        self.crosshairs = {}
        self.measure_lines_1 = {}
        self.measure_lines_2 = {}
        self.measure_points = []

        self._rebuild_value_bar(); self._rebuild_plots(); self._install_mouse_tracker()

        self.btn_open.clicked.connect(self._open_txt)
        self.btn_bin2hex.clicked.connect(self._convert_bin_to_hex)
        self.btn_connect.clicked.connect(self._connect_serial)
        self.btn_disconnect.clicked.connect(self._disconnect_serial)
        self.btn_open_cfg.clicked.connect(self._open_config_dialog)
        self.btn_save_cfg.clicked.connect(self._save_cfg)
        self.btn_load_cfg.clicked.connect(self._load_cfg)

        self.port_cb.currentTextChanged.connect(self._on_connection_setting_changed)
        self.baud_cb.currentTextChanged.connect(self._on_connection_setting_changed)

    # ---------- UART受信インジケータ ----------
    def _start_uart_indicator(self):
        self.uart_indicator.setVisible(True)
        self._uart_blink_on = True
        self._uart_blink_timer.start()

    def _stop_uart_indicator(self):
        self._uart_blink_timer.stop()
        self.uart_indicator.setVisible(False)

    def _blink_uart_indicator(self):
        self._uart_blink_on = not self._uart_blink_on
        if self._uart_blink_on:
            self.uart_indicator.setStyleSheet(
                "background:#fff3cd;color:#856404;font-weight:bold;font-size:13px;"
                "padding:4px;border:1px solid #ffc107;border-radius:4px;")
        else:
            self.uart_indicator.setStyleSheet(
                "background:#fffdf0;color:#c9a227;font-weight:bold;font-size:13px;"
                "padding:4px;border:1px solid #ffeeba;border-radius:4px;")

    # ---------- グラフ高さ調整 ----------
    def _shrink_plots(self):
        self._plot_min_height = max(100, self._plot_min_height - 50)
        self._apply_plot_height()

    def _expand_plots(self):
        self._plot_min_height += 50
        self._apply_plot_height()

    def _apply_plot_height(self):
        for grp in self.plot_groups.values():
            grp.widget.setMinimumHeight(self._plot_min_height)

    # ---------- 設定保存通知 ----------
    def _show_save_notify(self):
        self.save_notify_label.setText("設定保存済み ✓")
        self._save_notify_timer.start(3000)

    # ---------- 再構築 ----------
    def _rebuild_value_bar(self):
        for i in reversed(range(self.value_bar_layout.count())):
            item = self.value_bar_layout.itemAt(i); w = item.widget() if item else None
            if w: w.setParent(None)
        self.value_cards.clear(); self.unit_edits.clear()
        self.name_by_index.clear()
        self.graph_name_by_index.clear(); self.graph_unit_by_index.clear()

        # 有効バイトを収集
        active_indices = []
        i = 0; n = self.cfg.frame_size
        while i < n:
            rule = self.cfg.byte_rules[i] if i < len(self.cfg.byte_rules) else ByteRule()
            if rule.enabled and rule.method_id != "ignore":
                active_indices.append(i)
            i += 1

        if not active_indices:
            return

        tbl = QtWidgets.QTableWidget(1, len(active_indices))
        tbl.setMaximumHeight(70)
        tbl.setMinimumHeight(50)
        tbl.verticalHeader().setVisible(False)
        tbl.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        tbl.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.NoSelection)
        tbl.setFocusPolicy(QtCore.Qt.FocusPolicy.NoFocus)
        tbl.setStyleSheet(
            "QTableWidget{border:1px solid #ccc;border-radius:4px;background:#fafafa;gridline-color:#eee;}"
            "QHeaderView::section{background:#f0f0f0;font-weight:bold;font-size:11px;padding:2px 6px;border:1px solid #ddd;}"
            "QTableWidget::item{font-family:Consolas,'Cascadia Code',monospace;font-size:14px;font-weight:bold;text-align:center;padding:2px 4px;}"
        )

        headers = []
        for col, idx in enumerate(active_indices):
            rule = self.cfg.byte_rules[idx] if idx < len(self.cfg.byte_rules) else ByteRule()
            name = rule.name.strip() or f"データ{idx+1}"
            unit = rule.graph_unit.strip()
            header = f"{name} [{unit}]" if unit else name
            headers.append(header)

            self.name_by_index[idx] = name
            self.graph_name_by_index[idx] = name
            self.graph_unit_by_index[idx] = unit

            init_text = f"- {unit}" if unit else "-"
            item = QtWidgets.QTableWidgetItem(init_text)
            item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            tbl.setItem(0, col, item)

            # value_cardsにはQTableWidgetItemのテキスト更新用にダミーラベルを使わず
            # 直接参照できるようにする

        tbl.setHorizontalHeaderLabels(headers)
        tbl.horizontalHeader().setStretchLastSection(False)
        tbl.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.Stretch)
        tbl.verticalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.Stretch)

        self.value_bar_layout.insertWidget(0, tbl)

        # value_cards に格納（_process_frame から参照される）
        # QTableWidgetItem はQLabel互換の .setText() がないので、ラッパーを使う
        self._value_table = tbl
        self._value_col_map: Dict[int, int] = {}  # byte_index -> column
        for col, idx in enumerate(active_indices):
            self._value_col_map[idx] = col
            # ダミーのQLabelの代わりにテーブルアイテムを使うためvalue_cardsを特殊化
            self.value_cards[idx] = None  # 後で _process_frame で _value_table 経由で更新

    def _update_value_card(self, idx: int, text: str):
        """値テーブルのセルを更新"""
        if not hasattr(self, '_value_table') or self._value_table is None:
            return
        col = self._value_col_map.get(idx)
        if col is None:
            return
        item = self._value_table.item(0, col)
        if item:
            item.setText(text)

    def _make_value_card(self, idx: int, name: str, unit: str) -> QtWidgets.QFrame:
        """互換用（現在は _rebuild_value_bar でテーブルを使用）"""
        frame = QtWidgets.QFrame(); frame.setFrameShape(QtWidgets.QFrame.StyledPanel)
        frame.setStyleSheet("QFrame{border:1px solid #ccc;border-radius:6px;background:#fafafa;} QLabel{font-size:12px;}")
        v = QtWidgets.QVBoxLayout(frame); v.setContentsMargins(8,6,8,6)
        title = QtWidgets.QLabel(f"{name}"); title.setStyleSheet("font-weight:bold;font-size:11px;color:#555;")
        init_text = f"- {unit}" if unit else "-"
        val = QtWidgets.QLabel(init_text); val.setObjectName("val"); val.setStyleSheet("font-family:Consolas,'Cascadia Code',monospace;font-size:16px;font-weight:bold;")
        v.addWidget(title); v.addWidget(val)
        return frame

    def _group_rules(self) -> Dict[str, List[int]]:
        groups: Dict[str, List[int]] = {}
        for i, r in enumerate(self.cfg.byte_rules[:self.cfg.frame_size]):
            if r.enabled and r.graph_enabled and r.method_id != "ignore":
                key = r.graph_group_name if r.graph_group_name else NO_GROUP_KEY
                groups.setdefault(key, []).append(i)
        if NO_GROUP_KEY in groups:
            solo_indices = groups.pop(NO_GROUP_KEY)
            for si in solo_indices:
                r = self.cfg.byte_rules[si]
                solo_name = f"_solo_{r.name.strip() or f'byte{si+1}'}"
                groups[solo_name] = [si]
        return groups

    def _rebuild_plots(self):
        for i in reversed(range(self.plot_layout.count())):
            item = self.plot_layout.itemAt(i); w = item.widget() if item else None
            if w: w.setParent(None)

        self.plot_groups: Dict[str, ChannelPlotGroup] = {}
        self.curve_colors: Dict[str, QtGui.QColor] = {}
        self.crosshairs = {}
        self.measure_lines_1 = {}
        self.measure_lines_2 = {}
        self._label_tick_map = {}

        palette = ["#ff4d4f","#2f80ed","#27ae60","#f39c12","#9b59b6","#16a085","#d35400","#34495e"]
        pal_idx = 0

        groups = self._group_rules()
        xlink = None
        for gname, indices in groups.items():
            first_rule = self.cfg.byte_rules[indices[0]]
            y_unit = first_rule.graph_unit.strip()

            if gname.startswith("_solo_"):
                display_title = first_rule.name.strip() or f"データ{indices[0]+1}"
            else:
                display_title = gname

            grp_plot = ChannelPlotGroup(display_title, y_unit, xlink=xlink, min_height=self._plot_min_height)
            grp_plot.update_axis_label(display_unit_label(self.cfg.x_display_unit))
            if xlink is None: xlink = grp_plot.widget

            wrapper = DraggablePlotContainer(gname, grp_plot)
            self.plot_layout.addWidget(wrapper)
            self.plot_groups[gname] = grp_plot

            # Y軸ラベルティック集約
            all_ticks: List[Tuple[float, str]] = []
            seen_tick_vals = set()

            for i in indices:
                r = self.cfg.byte_rules[i]
                src_name = r.name.strip() or f"データ{i+1}"
                color = QtGui.QColor(palette[pal_idx % len(palette)]); pal_idx += 1
                self.curve_colors[src_name] = color
                grp_plot.add_or_get_curve(src_name, src_name, color)

                ticks = build_y_tick_labels(r)
                if ticks:
                    tick_dict = {}
                    for tv, tl in ticks:
                        tick_dict[tv] = tl
                        if tv not in seen_tick_vals:
                            all_ticks.append((tv, tl))
                            seen_tick_vals.add(tv)
                    self._label_tick_map[src_name] = tick_dict

            # ラベルティックがあればY軸に設定
            if all_ticks:
                all_ticks.sort(key=lambda x: x[0])
                grp_plot.set_y_tick_labels(all_ticks)

            line = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("#888", width=1, style=QtCore.Qt.PenStyle.DashLine))
            grp_plot.widget.addItem(line); self.crosshairs[gname] = line

            ml1 = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("#d62728", width=2))
            ml1.setVisible(False); grp_plot.widget.addItem(ml1); self.measure_lines_1[gname] = ml1
            
            ml2 = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("#1f77b4", width=2))
            ml2.setVisible(False); grp_plot.widget.addItem(ml2); self.measure_lines_2[gname] = ml2

        self.plot_layout.addStretch()

    # ---------- カーソル ----------
    def _install_mouse_tracker(self):
        if getattr(self, "_mouse_scenes", None):
            for scene in self._mouse_scenes:
                try: 
                    scene.sigMouseMoved.disconnect(self._on_mouse_moved)
                    scene.sigMouseClicked.disconnect(self._on_mouse_clicked)
                except: pass
        self._mouse_scenes = []
        if not self.plot_groups: return
        for grp in self.plot_groups.values():
            scene = grp.widget.scene()
            if scene in self._mouse_scenes: continue
            try: 
                scene.sigMouseMoved.connect(self._on_mouse_moved)
                scene.sigMouseClicked.connect(self._on_mouse_clicked)
                self._mouse_scenes.append(scene)
            except: pass

    def closeEvent(self, e: QtGui.QCloseEvent) -> None:
        self._auto_save()
        if getattr(self, "_mouse_scenes", None):
            for scene in self._mouse_scenes:
                try: scene.sigMouseMoved.disconnect(self._on_mouse_moved)
                except: pass
            self._mouse_scenes = []
        self._stop_threads(); super().closeEvent(e)

    def _on_connection_setting_changed(self):
        self.cfg.port = self.port_cb.currentText().strip()
        try: self.cfg.baudrate = int(self.baud_cb.currentText())
        except: pass
        self._auto_save()

    def _auto_save(self):
        try:
            self.cfg.port = self.port_cb.currentText().strip()
            try: self.cfg.baudrate = int(self.baud_cb.currentText())
            except: pass
            path = self._current_cfg_path if self._current_cfg_path else _resolve_startup_config_path()
            self.cfg.save(path)
            self._show_save_notify()
        except Exception as ex:
            print("auto config save failed:", ex)

    def _reload_ports(self):
        if not hasattr(self, "port_cb") or self.port_cb is None: return
        cur = self.port_cb.currentText().strip()
        self.port_cb.blockSignals(True)
        try:
            self.port_cb.clear(); lst = available_ports(); self.port_cb.addItems(lst)
            if cur: self.port_cb.setCurrentText(cur)
            elif self.cfg.port: self.port_cb.setCurrentText(self.cfg.port)
            elif lst: self.port_cb.setCurrentIndex(0)
        finally: self.port_cb.blockSignals(False)

    def _stop_threads(self):
        if getattr(self, "serial_thread", None) is not None and self.serial_thread.isRunning():
            self.serial_thread.stop(); self.serial_thread.wait(1000)
        if getattr(self, "file_thread", None) is not None and self.file_thread.isRunning():
            self.file_thread.wait(1000)

    def _connect_serial(self):
        if serial is None: QtWidgets.QMessageBox.warning(self, "UART", "pyserial が見つかりません。"); return
        self._stop_threads(); self.buffer = RingBuffer(); self.current_time_sec = 0.0
        self.cfg.port = self.port_cb.currentText().strip()
        try: self.cfg.baudrate = int(self.baud_cb.currentText())
        except: self.cfg.baudrate = 115200
        if not self.cfg.port: QtWidgets.QMessageBox.warning(self, "UART", "ポートが未指定です。"); return
        self.serial_thread = SerialReader(self.cfg, self)
        self.serial_thread.new_frame.connect(self._on_new_frame); self.serial_thread.start()
        self._start_uart_indicator()
        self.file_name_label.setText("ファイル: -")
        self.status_label.setText(f"状態: UART接続中 ({self.cfg.port})")

    def _disconnect_serial(self):
        self._stop_threads()
        self._stop_uart_indicator()
        self.status_label.setText("状態: 切断")

    def _open_txt(self):
        self._stop_threads()
        self._stop_uart_indicator()
        # 設定フォルダ(setting)は参照しない
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "ログ(.txt)を開く",
            "",
            "Text (*.txt)"
        )
        if not path: return
        self.buffer = RingBuffer(); self.current_time_sec = 0.0
        self.file_thread = TxtFileReader(path, self.cfg, self)
        self.file_thread.new_frame.connect(self._on_new_file_frame)
        self.file_thread.finished.connect(lambda: self.status_label.setText("状態: 読込完了"))
        self.file_thread.start()
        filename = os.path.basename(path)
        self.file_name_label.setText(f"ファイル: {filename}")
        self.status_label.setText(f"状態: 読込中 ({filename})")

    def _convert_bin_to_hex(self):
        """バイナリ .txt ファイルを読み込み、各バイトをHEX文字列に変換して保存する。"""
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "バイナリ .txt を選択",
            "",
            "Text (*.txt);;All Files (*)"
        )
        if not path:
            return

        try:
            with open(path, "rb") as f:
                raw = f.read()
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "読込エラー", f"ファイルを読み込めませんでした:\n{e}")
            return

        if not raw:
            QtWidgets.QMessageBox.information(self, "変換", "ファイルが空です。")
            return

        # フレームサイズごとに1行としてHEX出力
        fs = self.cfg.frame_size
        lines = []
        for offset in range(0, len(raw), fs):
            chunk = raw[offset:offset + fs]
            hex_str = " ".join(f"0x{b:02X}" for b in chunk)
            lines.append(hex_str)

        # 出力ファイル名: 元ファイル名_hex変換.txt
        base, ext = os.path.splitext(path)
        out_path = f"{base}_hex変換{ext}"

        try:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "保存エラー", f"ファイルを保存できませんでした:\n{e}")
            return

        QtWidgets.QMessageBox.information(
            self, "変換完了",
            f"HEX変換が完了しました。\n\n保存先:\n{out_path}\n\n"
            f"フレームサイズ: {fs} バイト/行\n"
            f"総バイト数: {len(raw)}\n"
            f"出力行数: {len(lines)}"
        )
        self.status_label.setText(f"状態: HEX変換完了 ({os.path.basename(out_path)})")

    def _process_frame(self, frame: bytes, sample_index: int):
        if len(frame) != self.cfg.frame_size: return

        vals_by_name = evaluate_byte_values(frame, self.cfg)
        labels_by_name = evaluate_byte_labels(frame, self.cfg)

        for idx in list(self.value_cards.keys()):
            src = self.name_by_index.get(idx, f"byte{idx+1}")
            unit = self.graph_unit_by_index.get(idx, "")
            unit_str = f" {unit}" if unit else ""
            label_str = labels_by_name.get(src, "")
            if label_str:
                self._update_value_card(idx, f"{label_str}{unit_str}")
            else:
                v = vals_by_name.get(src, float("nan"))
                if v != v:
                    self._update_value_card(idx, "NaN")
                else:
                    self._update_value_card(idx, f"{v:g}{unit_str}")

        plot_values: Dict[str, float] = {}
        active_plot_names: List[str] = []
        for i, r in enumerate(self.cfg.byte_rules[:self.cfg.frame_size]):
            if r.enabled and r.graph_enabled and r.method_id != "ignore":
                src = r.name.strip() or f"データ{i+1}"
                active_plot_names.append(src)
                if src in vals_by_name:
                    plot_values[src] = vals_by_name[src]

        self.buffer.append(sample_index, plot_values, active_plot_names)

    @QtCore.Slot(bytes, int)
    def _on_new_file_frame(self, frame: bytes, index: int):
        self._process_frame(frame, index)

    @QtCore.Slot(bytes)
    def _on_new_frame(self, frame: bytes):
        self._process_frame(frame, len(self.buffer.t))

    def _open_config_dialog(self):
        dlg = ConfigDialog(self.cfg, self)
        if dlg.exec():
            try: new_cfg = dlg.get_config()
            except: return
            new_cfg.port = self.port_cb.currentText().strip()
            try: new_cfg.baudrate = int(self.baud_cb.currentText())
            except: new_cfg.baudrate = self.cfg.baudrate
            self.cfg = new_cfg
            self._rebuild_value_bar(); self._rebuild_plots()
            for grp in self.plot_groups.values(): grp.update_axis_label(display_unit_label(self.cfg.x_display_unit))
            self._install_mouse_tracker()
            self._auto_save()
            self.status_label.setText("状態: 設定を適用")

    def _save_cfg(self):
        default = self._current_cfg_path if self._current_cfg_path else _resolve_startup_config_path()
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "設定を保存", default, "JSON (*.json)")
        if not path: return
        self.cfg.port = self.port_cb.currentText().strip()
        try: self.cfg.baudrate = int(self.baud_cb.currentText())
        except: pass
        self.cfg.save(path)
        self._current_cfg_path = path
        self._show_save_notify()
        self.status_label.setText(f"状態: 設定を保存 ({os.path.basename(path)})")

    def _load_cfg(self):
        if not os.path.isdir(SETTINGS_DIR):
            os.makedirs(SETTINGS_DIR, exist_ok=True)

        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "設定を読込",
            SETTINGS_DIR,
            "JSON (*.json)"
        )
        if not path:
            return
        try:
            cfg = AppConfig.load(path)
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "設定読込", f"読込に失敗しました: {e}")
            return

        self.cfg = cfg
        self._current_cfg_path = path
        self.port_cb.blockSignals(True)
        self.baud_cb.blockSignals(True)
        self.port_cb.setCurrentText(self.cfg.port)
        self.baud_cb.setCurrentText(str(self.cfg.baudrate))
        self.port_cb.blockSignals(False)
        self.baud_cb.blockSignals(False)
        self._rebuild_value_bar()
        self._rebuild_plots()
        for grp in self.plot_groups.values():
            grp.update_axis_label(display_unit_label(self.cfg.x_display_unit))
        self._install_mouse_tracker()
        self.status_label.setText(f"状態: 設定を読込 ({os.path.basename(path)})")

    def _refresh_plots(self):
        if not self.buffer.t:
            return
        dt = to_seconds(self.cfg.sample_interval_value, self.cfg.sample_interval_unit)
        if dt <= 0:
            dt = 0.001
        scale = display_scale_for(self.cfg.x_display_unit)
        xs_disp = np.array(self.buffer.t, dtype=np.float64) * (dt * scale)

        for grp in self.plot_groups.values():
            for src_name, curve in grp.curves.items():
                raw = self.buffer.series.get(src_name)
                if not raw:
                    continue
                ys = np.array(raw, dtype=np.float64)
                # 長さ差が出ても安全に末尾合わせ
                n = min(len(xs_disp), len(ys))
                if n <= 0:
                    continue
                curve.setData(xs_disp[-n:], ys[-n:])

    def _on_mouse_moved(self, pos: QtCore.QPointF):
        if not self.plot_groups: return
        # データが空なら何もしない（グラフスケールが変わるのを防止）
        if not self.buffer.t: return

        plot_under_mouse = None
        for gname, grp in self.plot_groups.items():
            if grp.widget.sceneBoundingRect().contains(pos): plot_under_mouse = grp.widget; break
        if plot_under_mouse is None: return
        vb = plot_under_mouse.getViewBox(); mp = vb.mapSceneToView(pos); x_disp = float(mp.x())

        indices = list(self.buffer.t)
        if not indices: return

        dt = to_seconds(self.cfg.sample_interval_value, self.cfg.sample_interval_unit)
        if dt <= 0: dt = 0.001
        scale = display_scale_for(self.cfg.x_display_unit)
        if scale == 0 or dt == 0: return

        # カーソルXをデータ範囲にクリップ
        x_min = float(indices[0]) * dt * scale
        x_max = float(indices[-1]) * dt * scale
        x_clamped = max(x_min, min(x_max, x_disp))

        for line in self.crosshairs.values():
            try: line.setPos(x_clamped)
            except: pass

        x_as_index = x_clamped / (dt * scale)
        nearest_idx = min(range(len(indices)), key=lambda k: abs(float(indices[k]) - x_as_index))
        t_disp = float(indices[nearest_idx]) * dt * scale

        # 単位の辞書を事前構築
        unit_by_name: Dict[str, str] = {}
        for i, r in enumerate(self.cfg.byte_rules[:self.cfg.frame_size]):
            nm = r.name.strip() or f"データ{i+1}"
            unit_by_name[nm] = r.graph_unit.strip()

        vals_html = []
        for gid, grp in sorted(self.plot_groups.items()):
            for src_name in sorted(grp.curves.keys()):
                ys = self.buffer.series.get(src_name, [])
                if not ys or nearest_idx >= len(ys):
                    continue
                v = ys[nearest_idx]
                col = self.curve_colors.get(src_name, QtGui.QColor("#000000"))
                col_hex = col.name()
                # ラベルマップがあればラベル表示、なければ数値
                tick_dict = self._label_tick_map.get(src_name)
                if tick_dict and v in tick_dict:
                    disp_val = tick_dict[v]
                else:
                    disp_val = f"{v:g}"
                unit = unit_by_name.get(src_name, "")
                unit_str = f" {unit}" if unit else ""
                vals_html.append(
                    f"<span style='color:{col_hex};'>"
                    f"{html_escape(src_name)}={html_escape(disp_val)}{html_escape(unit_str)}"
                    f"</span>"
                )

        x_unit = display_unit_label(self.cfg.x_display_unit)
        header = f"カーソル: 時間 {t_disp:.3f} {x_unit}  |  "

        self.readout_label.setTextFormat(QtCore.Qt.TextFormat.RichText)
        self.readout_label.setText(header + (" | ".join(vals_html) if vals_html else "-"))

    def _on_mouse_clicked(self, ev):
        if ev.button() != QtCore.Qt.MouseButton.LeftButton: return
        if not self.plot_groups or not self.buffer.t: return

        pos = ev.scenePos()
        plot_under_mouse = None
        for gname, grp in self.plot_groups.items():
            if grp.widget.sceneBoundingRect().contains(pos): 
                plot_under_mouse = grp.widget
                break
        if plot_under_mouse is None: return

        vb = plot_under_mouse.getViewBox()
        mp = vb.mapSceneToView(pos)
        x_val = float(mp.x())

        # 3点目のクリックでリセット（線とデータを消して終了）
        if len(self.measure_points) >= 2:
            self.measure_points.clear()
            for line in self.measure_lines_1.values(): line.setVisible(False)
            for line in self.measure_lines_2.values(): line.setVisible(False)
            self.measure_label.setText("測定: -")
            return  # ここで処理を終わらせることで1点目として登録されない

        self.measure_points.append(x_val)

        if len(self.measure_points) == 1:
            for line in self.measure_lines_1.values():
                line.setPos(x_val); line.setVisible(True)
            self.measure_label.setText("測定: 2点目をクリック...")
        elif len(self.measure_points) == 2:
            for line in self.measure_lines_2.values():
                line.setPos(x_val); line.setVisible(True)
            diff = abs(self.measure_points[1] - self.measure_points[0])
            x_unit = display_unit_label(self.cfg.x_display_unit)
            self.measure_label.setText(f"測定: Δt = {diff:.3f} {x_unit}")

def html_escape(s: str) -> str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;").replace("'","&#39;")

def _apply_config_to_window(w: MainWindow, cfg: AppConfig):
    w.cfg = cfg
    w.port_cb.blockSignals(True)
    w.baud_cb.blockSignals(True)
    w.port_cb.setCurrentText(w.cfg.port); w.baud_cb.setCurrentText(str(w.cfg.baudrate))
    w.port_cb.blockSignals(False)
    w.baud_cb.blockSignals(False)
    w._rebuild_value_bar(); w._rebuild_plots()
    for grp in w.plot_groups.values(): grp.update_axis_label(display_unit_label(w.cfg.x_display_unit))
    w._install_mouse_tracker()

def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()

    startup_cfg_path = _resolve_startup_config_path()
    w._current_cfg_path = startup_cfg_path

    if os.path.exists(startup_cfg_path):
        try:
            cfg = AppConfig.load(startup_cfg_path)
            _apply_config_to_window(w, cfg)
            w.status_label.setText(f"状態: 設定を自動読込 ({os.path.basename(startup_cfg_path)})")
        except Exception as e:
            print("auto config load skipped:", e)
    else:
        try:
            default_cfg = AppConfig()
            default_cfg.save(startup_cfg_path)
            w.status_label.setText(f"状態: 設定ファイルを新規作成 ({os.path.basename(startup_cfg_path)})")
        except Exception as e:
            print("auto config create failed:", e)

    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":    main()