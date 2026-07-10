# -*- coding: utf-8 -*-
"""
修改 sparkppt.pptx：保留前6个幻灯片不动，在后3个幻灯片（7-9）中介绍
Kafka Consumer、Producer 和 Web index.jsp 的实现。
"""
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE
import copy

PPTX_PATH = 'src/main/resources/sparkppt.pptx'

prs = Presentation(PPTX_PATH)

# ============================================================
# 辅助函数
# ============================================================
def add_textbox(slide, left, top, width, height):
    """在幻灯片上添加一个文本框"""
    return slide.shapes.add_textbox(left, top, width, height)

def set_paragraph(para, text, size=Pt(14), bold=False, color=RGBColor(0x1a, 0x1a, 0x2e), alignment=PP_ALIGN.LEFT, spacing=Pt(6)):
    """设置段落文本和格式"""
    para.text = text
    para.space_after = spacing
    para.alignment = alignment
    if para.runs:
        run = para.runs[0]
    else:
        run = para.add_run()
        run.text = text
    run.font.size = size
    run.font.bold = bold
    run.font.color.rgb = color
    # 重新设置文本（因为 add_run 会追加）
    para.clear()
    run = para.add_run()
    run.text = text
    run.font.size = size
    run.font.bold = bold
    run.font.color.rgb = color
    return run

def add_title_text(slide, text, top=Inches(0.3)):
    """在幻灯片上添加一个大标题"""
    txBox = add_textbox(slide, Inches(0.6), top, Inches(8.5), Inches(0.7))
    tf = txBox.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    set_paragraph(p, text, size=Pt(28), bold=True, color=RGBColor(0x1a, 0x56, 0xdb), alignment=PP_ALIGN.LEFT)
    return txBox

def add_subtitle(slide, text, top=Inches(0.9)):
    """添加副标题"""
    txBox = add_textbox(slide, Inches(0.8), top, Inches(8.0), Inches(0.4))
    tf = txBox.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    set_paragraph(p, text, size=Pt(14), bold=False, color=RGBColor(0x58, 0x66, 0x94), alignment=PP_ALIGN.LEFT)
    return txBox

def add_body_text(slide, lines, left=Inches(0.6), top=Inches(1.5), width=Inches(8.5), height=Inches(4.8)):
    """添加正文内容，lines 是一个列表，每个元素是 (text, indent_level, bold, size)"""
    txBox = add_textbox(slide, left, top, width, height)
    tf = txBox.text_frame
    tf.word_wrap = True

    for i, line in enumerate(lines):
        text = line[0]
        indent = line[1] if len(line) > 1 else 0
        bold = line[2] if len(line) > 2 else False
        size = line[3] if len(line) > 3 else Pt(13)

        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()

        p.level = indent
        p.space_after = Pt(4)
        p.space_before = Pt(2)
        run = p.add_run()
        run.text = text
        run.font.size = size
        run.font.bold = bold
        run.font.color.rgb = RGBColor(0xe6, 0xed, 0xf3)
    return txBox

def add_code_block(slide, code_lines, top, left=Inches(0.6), width=Inches(4.2), height=None):
    """添加代码块样式文本框"""
    if height is None:
        height = Inches(0.3) * len(code_lines) + Inches(0.3)

    txBox = add_textbox(slide, left, top, width, height)
    tf = txBox.text_frame
    tf.word_wrap = True

    # 代码块背景（通过设置文本框填充）
    txBox.fill.solid()
    txBox.fill.fore_color.rgb = RGBColor(0x0d, 0x11, 0x17)

    for i, code_line in enumerate(code_lines):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()

        p.space_after = Pt(1)
        p.space_before = Pt(1)
        run = p.add_run()
        run.text = code_line
        run.font.size = Pt(10)
        run.font.name = 'Consolas'
        run.font.color.rgb = RGBColor(0x7e, 0xe7, 0x87)  # 绿色代码
    return txBox

def add_section_header(slide, text, left=Inches(0.6), top=Inches(1.4), width=Inches(8.5)):
    """添加带颜色条的章节标题"""
    # 颜色条
    bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, left, top, Inches(0.08), Inches(0.35)
    )
    bar.fill.solid()
    bar.fill.fore_color.rgb = RGBColor(0x1a, 0x56, 0xdb)
    bar.line.fill.background()

    # 文字
    txBox = add_textbox(slide, left + Inches(0.2), top, width, Inches(0.35))
    tf = txBox.text_frame
    p = tf.paragraphs[0]
    set_paragraph(p, text, size=Pt(18), bold=True, color=RGBColor(0x58, 0xa6, 0xff))
    return txBox

def set_slide_bg(slide, color=RGBColor(0x0d, 0x11, 0x17)):
    """设置幻灯片背景为深色"""
    background = slide.background
    fill = background.fill
    fill.solid()
    fill.fore_color.rgb = color


# ============================================================
# 获取要修改的幻灯片（7, 8, 9）
# ============================================================
slides = list(prs.slides)

# 确保至少有 9 个幻灯片
while len(slides) < 9:
    # 不太可能，但以防万一
    slide_layout = prs.slide_layouts[1]  # Title and Content
    prs.slides.add_slide(slide_layout)
    slides = list(prs.slides)

slide7 = slides[6]  # 第7个幻灯片
slide8 = slides[7]  # 第8个幻灯片
slide9 = slides[8]  # 第9个幻灯片

# ============================================================
# 幻灯片 7：Kafka Producer 实现
# ============================================================
set_slide_bg(slide7)
# 清除原有占位符内容
for shape in slide7.shapes:
    if shape.has_text_frame:
        shape.text_frame.clear()

add_title_text(slide7, 'Kafka Producer — 数据生产端实现')
add_subtitle(slide7, '从 MySQL 读取房屋数据 → 过滤 → 发送到 Kafka → 断点续跑', top=Inches(0.85))

# ---- 左侧：架构说明 ----
add_section_header(slide7, '▎架构概述', top=Inches(1.3))

lines_left = [
    ('📍 数据源：MySQL cjz_spark.house_info_clean_checkid 表', 0, False, Pt(12)),
    ('📤 目标：Kafka Topic "kafka"（3节点集群）', 0, False, Pt(12)),
    ('', 0, False, Pt(6)),
    ('核心流程：', 0, True, Pt(13)),
    ('  ① 读取上次处理进度 (kafka_producer_progress 表)', 0, False, Pt(12)),
    ('  ② 按 checkid 自增轮询，每次查询该批次数据', 0, False, Pt(12)),
    ('  ③ 过滤条件：district="海淀" AND area∈[90,144) ㎡', 0, False, Pt(12)),
    ('  ④ 逐条构造 JSON 消息 → KafkaProducer.send()', 0, False, Pt(12)),
    ('  ⑤ 发送批次结束标记 __END_OF_BATCH__', 0, False, Pt(12)),
    ('  ⑥ 持久化进度 (kafka_producer_progress)', 0, False, Pt(12)),
    ('  ⑦ checkid++，间隔 1 秒继续轮询', 0, False, Pt(12)),
]
add_body_text(slide7, lines_left, left=Inches(0.6), top=Inches(1.8), width=Inches(5.2), height=Inches(4.2))

# ---- 右侧：关键配置 ----
add_section_header(slide7, '▎关键配置与特性', left=Inches(6.0), top=Inches(1.3))

lines_right = [
    ('Kafka 配置：', 0, True, Pt(12)),
    ('  • bootstrap.servers：hadoop101:9092,', 0, False, Pt(10)),
    ('    niit-slaves1:9092, niit-slaves2:9092', 0, False, Pt(10)),
    ('  • acks=all（最高可靠性）', 0, False, Pt(10)),
    ('  • retries=3（失败重试）', 0, False, Pt(10)),
    ('  • 序列化：StringSerializer', 0, False, Pt(10)),
    ('', 0, False, Pt(4)),
    ('消息格式（JSON）：', 0, True, Pt(12)),
    ('  {"community":"世茂城",', 0, False, Pt(10)),
    ('   "price_per_sqm":378810}', 0, False, Pt(10)),
    ('', 0, False, Pt(4)),
    ('批次结束标记：', 0, True, Pt(12)),
    ('  {"__END_OF_BATCH__":true,', 0, False, Pt(10)),
    ('   "checkid":123}', 0, False, Pt(10)),
    ('', 0, False, Pt(4)),
    ('断点续跑：', 0, True, Pt(12)),
    ('  • 进度表 kafka_producer_progress', 0, False, Pt(10)),
    ('  • 重启后自动从上次 checkid+1 开始', 0, False, Pt(10)),
]
add_body_text(slide7, lines_right, left=Inches(6.0), top=Inches(1.8), width=Inches(3.8), height=Inches(4.5))

# ============================================================
# 幻灯片 8：Kafka Consumer 实现
# ============================================================
set_slide_bg(slide8)
for shape in slide8.shapes:
    if shape.has_text_frame:
        shape.text_frame.clear()

add_title_text(slide8, 'Kafka Consumer — 数据消费端实现')
add_subtitle(slide8, '消费 Kafka 消息 → 内存聚合（ConcurrentHashMap）→ 批量写入 MySQL', top=Inches(0.85))

# ---- 左侧：架构说明 ----
add_section_header(slide8, '▎架构概述', top=Inches(1.3))

lines_left = [
    ('📥 数据源：Kafka Topic "kafka"（earliest 策略）', 0, False, Pt(12)),
    ('💾 目标：MySQL cjz_spark.certain_analysis 表', 0, False, Pt(12)),
    ('', 0, False, Pt(6)),
    ('消费流程：', 0, True, Pt(13)),
    ('  ① 订阅 Topic，poll(Duration.ofMillis(100)) 拉取消息', 0, False, Pt(12)),
    ('  ② 判断消息类型：', 0, False, Pt(12)),
    ('     • 普通消息 → 解析 community + price_per_sqm', 0, False, Pt(12)),
    ('     • __END_OF_BATCH__ → 触发 MySQL 批量写入', 0, False, Pt(12)),
    ('  ③ 内存聚合：ConcurrentHashMap<String, long[]>', 0, False, Pt(12)),
    ('     Key = 小区名，Value = [房屋数量, 单价总和]', 0, False, Pt(12)),
    ('  ④ 收到结束标记后，批量写入 MySQL', 0, False, Pt(12)),
    ('  ⑤ 清空内存容器，准备下一批次', 0, False, Pt(12)),
]
add_body_text(slide8, lines_left, left=Inches(0.6), top=Inches(1.8), width=Inches(5.2), height=Inches(4.2))

# ---- 右侧：关键特性 ----
add_section_header(slide8, '▎关键实现细节', left=Inches(6.0), top=Inches(1.3))

lines_right = [
    ('Kafka 配置：', 0, True, Pt(12)),
    ('  • GROUP_ID_CONFIG = "kafka"', 0, False, Pt(10)),
    ('  • AUTO_OFFSET_RESET = "earliest"', 0, False, Pt(10)),
    ('  • 反序列化：StringDeserializer', 0, False, Pt(10)),
    ('', 0, False, Pt(4)),
    ('内存聚合（ConcurrentHashMap）：', 0, True, Pt(12)),
    ('  • 线程安全的 compute() 方法', 0, False, Pt(10)),
    ('  • 原子性累加：count + 1, sum + price', 0, False, Pt(10)),
    ('  • 最终 avgPrice = sum / count', 0, False, Pt(10)),
    ('', 0, False, Pt(4)),
    ('MySQL 写入（幂等）：', 0, True, Pt(12)),
    ('  INSERT ... ON DUPLICATE KEY UPDATE', 0, False, Pt(10)),
    ('  number = number + VALUES(number),', 0, False, Pt(10)),
    ('  averageprice = 加权平均公式', 0, False, Pt(10)),
    ('  重复执行不脏数据 ✅', 0, False, Pt(10)),
    ('', 0, False, Pt(4)),
    ('简单 JSON 解析：', 0, True, Pt(12)),
    ('  • 不依赖第三方 JSON 库', 0, False, Pt(10)),
    ('  • extractJsonValue() 手写解析', 0, False, Pt(10)),
]
add_body_text(slide8, lines_right, left=Inches(6.0), top=Inches(1.8), width=Inches(3.8), height=Inches(4.8))

# ---- 底部：数据链路图 ----
add_section_header(slide8, '▎数据链路', top=Inches(5.6))

flow_lines = [
    ('MySQL (house_info_clean_checkid)  →  Producer  →  Kafka  →  Consumer  →  MySQL (certain_analysis)  →  前端 Dashboard（3秒刷新）', 0, True, Pt(14)),
]
add_body_text(slide8, flow_lines, left=Inches(0.6), top=Inches(6.05), width=Inches(9.0), height=Inches(0.5))

# ============================================================
# 幻灯片 9：Web 可视化 Dashboard (index.jsp)
# ============================================================
set_slide_bg(slide9)
for shape in slide9.shapes:
    if shape.has_text_frame:
        shape.text_frame.clear()

add_title_text(slide9, 'Web 可视化 — 房屋价格分析 Dashboard')
add_subtitle(slide9, 'index.jsp + ECharts + AJAX 轮询 → 深色主题实时数据大屏', top=Inches(0.85))

# ---- 左侧：5个图表面板 ----
add_section_header(slide9, '▎5 个图表面板', top=Inches(1.3))

lines_left = [
    ('① 面积区间房价分析（左上）', 0, True, Pt(12)),
    ('   分组柱状图：area_range × price_level，4×4 维度', 0, False, Pt(11)),
    ('   数据源：/data?table=area（Spark 离线分析）', 0, False, Pt(11)),
    ('', 0, False, Pt(3)),
    ('② 市区房价对比（右上）', 0, True, Pt(12)),
    ('   柱状图+折线图：房价与房屋数量双轴展示', 0, False, Pt(11)),
    ('   数据源：/data?table=district（Spark 离线分析）', 0, False, Pt(11)),
    ('', 0, False, Pt(3)),
    ('③ 年份区间户型分布（中左）', 0, True, Pt(12)),
    ('   堆叠柱状图：小/中/大户型占比', 0, False, Pt(11)),
    ('   数据源：/data?table=year（Spark 离线分析）', 0, False, Pt(11)),
    ('', 0, False, Pt(3)),
    ('④ 小区房价 Top20（中右）', 0, True, Pt(12)),
    ('   横向柱状图：均价最高小区排行', 0, False, Pt(11)),
    ('   数据源：/data?table=community（Spark 离线分析）', 0, False, Pt(11)),
    ('', 0, False, Pt(3)),
    ('⑤ 实时小区分析（底部全宽）🔴', 0, True, Pt(12)),
    ('   横向柱状图：海淀区 90-144㎡ 小区实时均价', 0, False, Pt(11)),
    ('   数据源：/data?table=certain（Kafka 实时流）', 0, False, Pt(11)),
    ('   每 3 秒 AJAX 轮询刷新', 0, False, Pt(11)),
]
add_body_text(slide9, lines_left, left=Inches(0.6), top=Inches(1.8), width=Inches(5.5), height=Inches(4.5))

# ---- 右侧：技术栈与特性 ----
add_section_header(slide9, '▎技术栈与关键特性', left=Inches(6.2), top=Inches(1.3))

lines_right = [
    ('前端技术栈：', 0, True, Pt(12)),
    ('  • ECharts 5.4.3（CDN 引入）', 0, False, Pt(11)),
    ('  • 原生 JavaScript（无框架依赖）', 0, False, Pt(11)),
    ('  • CSS Grid 深色主题布局', 0, False, Pt(11)),
    ('  • JSP 动态路径：<%= request.getContextPath() %>', 0, False, Pt(11)),
    ('', 0, False, Pt(4)),
    ('数据加载：', 0, True, Pt(12)),
    ('  • fetchData(table) → /data?table=xxx', 0, False, Pt(11)),
    ('  • 离线数据：页面加载时一次性获取', 0, False, Pt(11)),
    ('  • 实时数据：setInterval(pollRealtime, 3000)', 0, False, Pt(11)),
    ('', 0, False, Pt(4)),
    ('深色主题设计：', 0, True, Pt(12)),
    ('  • 背景色：#0d1117（GitHub 风格暗色）', 0, False, Pt(11)),
    ('  • darkThemeOptions() 统一配置', 0, False, Pt(11)),
    ('  • 渐变色柱状图 + 圆角 + 标签', 0, False, Pt(11)),
    ('', 0, False, Pt(4)),
    ('响应式设计：', 0, True, Pt(12)),
    ('  • window.resize → 所有图表自适应', 0, False, Pt(11)),
    ('  • 实时时钟更新（每秒刷新）', 0, False, Pt(11)),
    ('  • LIVE 动画指示器（实时面板）', 0, False, Pt(11)),
]
add_body_text(slide9, lines_right, left=Inches(6.2), top=Inches(1.8), width=Inches(3.6), height=Inches(4.8))

# ---- 底部：数据流总结 ----
add_section_header(slide9, '▎完整数据流链路', top=Inches(5.8))

flow_lines = [
    ('MySQL 源数据  →  Kafka Producer  →  Kafka 集群  →  Kafka Consumer  →  MySQL 结果表  →  /data 接口  →  AJAX 轮询  →  ECharts 实时渲染', 0, True, Pt(13)),
]
add_body_text(slide9, flow_lines, left=Inches(0.6), top=Inches(6.2), width=Inches(9.2), height=Inches(0.5))

# ============================================================
# 保存
# ============================================================
output_path = 'src/main/resources/sparkppt_new.pptx'
prs.save(output_path)
print('Saved to: ' + output_path)
print('Slides 1-6: unchanged')
print('Slide 7: Kafka Producer')
print('Slide 8: Kafka Consumer')
print('Slide 9: Web Dashboard (index.jsp)')
print('Slides 10-11: unchanged')
