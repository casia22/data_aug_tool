# app.py
from flask import Flask, render_template, request, jsonify
import mysql.connector
import os
import json
from datetime import datetime, timedelta, date, time
import math # 用于时间戳处理
import random # <--- 添加这个导入

app = Flask(__name__)

# --- Database Configuration ---
# !! IMPORTANT: Use environment variables or a config file in production!
DB_CONFIG = {
    'host': 'localhost',  # Or your MySQL host
    'user': 'root',
    'password': '8088178', # Replace with your actual password
    'database': 'pingtan'
}
# Replace 'YOUR_MYSQL_PASSWORD' with 8088178

# Helper function to format seconds into HH:MM:SS
def format_duration_seconds(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "00:00:00"
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

@app.route('/')
def index():
    """主页 - 只加载基本数据，详细统计通过AJAX按需加载"""
    results = []
    headers = ['日期', '记录数量', '开始时间', '结束时间', '时长(小时)'] # 基本列
    error = None
    conn = None
    
    try:
        # Connect to the database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True) # Fetch results as dictionaries

        # 使用基本查询 - 这个查询速度快，不涉及复杂计算
        cursor.execute("""
        SELECT
            data_date AS '日期',
            COUNT(*) AS '记录数量',
            DATE_FORMAT(MIN(create_time), '%H:%i:%s') AS '开始时间',
            DATE_FORMAT(MAX(create_time), '%H:%i:%s') AS '结束时间',
            ROUND(TIMESTAMPDIFF(SECOND, MIN(create_time), MAX(create_time))/3600, 2) AS '时长(小时)'
        FROM
            定位表
        GROUP BY
            data_date
        ORDER BY
            data_date;
        """)
        results = cursor.fetchall()

    except mysql.connector.Error as err:
        error = f"Database Error: {err}"
        app.logger.error(f"Database error: {err}") # Log the error
    except Exception as e:
        error = f"An unexpected error occurred: {e}"
        app.logger.error(f"Unexpected error: {e}")
    finally:
        # Ensure the connection is closed
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

    return render_template('index.html', results=results, headers=headers, error=error)

@app.route('/api/calculate_advanced_stats', methods=['POST'])
def calculate_advanced_stats():
    """API端点 - 执行详细的时间和段落统计（耗时操作）"""
    try:
        data = request.get_json()
        if not data or 'dates' not in data:
            return jsonify({'error': '没有提供日期列表'}), 400
            
        dates = data['dates']
        if not dates:
            return jsonify({'error': '日期列表为空'}), 400
            
        # 设置段落检测间隔阈值（秒）
        gap_threshold_seconds = data.get('gap_threshold', 300)  # 默认改为5分钟，之前是60秒
        
        stats_results = {}
        
        conn = mysql.connector.connect(**DB_CONFIG)
        
        for date in dates:
            # 为每个日期查询时间戳
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT create_time 
                FROM 定位表 
                WHERE data_date = %s 
                ORDER BY create_time
            """, (date,))
            timestamps = [row['create_time'] for row in cursor.fetchall() if row['create_time']]
            cursor.close()
            
            date_stats = {
                '记录数量': len(timestamps),
                '真实录制时长': '00:00:00',
                '数据段数': 0
            }
            
            # 如果有数据，计算详细统计
            if timestamps:
                # 计算开始/结束时间
                start_time = timestamps[0]
                end_time = timestamps[-1]
                total_duration_simple = (end_time - start_time).total_seconds()
                date_stats['总时长(最大-最小)'] = format_duration_seconds(total_duration_simple)
                
                # 计算实际录制时长和段数
                real_duration_seconds = 0
                segments = []
                
                if len(timestamps) > 1:
                    segment_start_time = timestamps[0]
                    segment_start_idx = 0
                    current_segment = {'start_time': segment_start_time.strftime('%H:%M:%S')}
                    
                    for i in range(1, len(timestamps)):
                        time_diff = (timestamps[i] - timestamps[i-1]).total_seconds()
                        
                        if time_diff > gap_threshold_seconds:
                            # 发现间隙，结束当前段落
                            segment_duration = (timestamps[i-1] - segment_start_time).total_seconds()
                            real_duration_seconds += segment_duration
                            
                            # 保存段落信息
                            current_segment['end_time'] = timestamps[i-1].strftime('%H:%M:%S')
                            current_segment['duration'] = format_duration_seconds(segment_duration)
                            current_segment['points'] = i - segment_start_idx
                            segments.append(current_segment)
                            
                            # 开始新段落
                            segment_start_time = timestamps[i]
                            segment_start_idx = i
                            current_segment = {'start_time': segment_start_time.strftime('%H:%M:%S')}
                    
                    # 处理最后一个段落
                    segment_duration = (timestamps[-1] - segment_start_time).total_seconds()
                    real_duration_seconds += segment_duration
                    
                    current_segment['end_time'] = timestamps[-1].strftime('%H:%M:%S')
                    current_segment['duration'] = format_duration_seconds(segment_duration)
                    current_segment['points'] = len(timestamps) - segment_start_idx
                    segments.append(current_segment)
                
                elif len(timestamps) == 1:
                    # 只有一个点
                    segments.append({
                        'start_time': timestamps[0].strftime('%H:%M:%S'),
                        'end_time': timestamps[0].strftime('%H:%M:%S'),
                        'duration': '00:00:00',
                        'points': 1
                    })
                    
                date_stats['真实录制时长'] = format_duration_seconds(real_duration_seconds)
                date_stats['数据段数'] = len(segments)
                date_stats['segments'] = segments  # 包含各段详情
            
            stats_results[date] = date_stats
            
        conn.close()
        return jsonify({'success': True, 'results': stats_results})
        
    except mysql.connector.Error as err:
        app.logger.error(f"Database error in advanced stats: {err}")
        return jsonify({'error': f"数据库错误: {err}"}), 500
    except Exception as e:
        app.logger.error(f"Error in advanced stats: {e}")
        return jsonify({'error': f"计算错误: {e}"}), 500

# --- 轨迹查询 SQL ---
TRACK_QUERY = """
SELECT
    timestamp,
    create_time,
    lat,
    lon,
    vehicle_heading_angle,
    longitudinal_acceleration,
    lateral_acceleration,
    x,
    y,
    z
FROM
    定位表
WHERE
    data_date = %s
ORDER BY
    create_time;
"""

@app.route('/track/<date>')
def track_visualization(date):
    track_data = []
    error = None
    conn = None
    
    try:
        # 连接数据库
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # 执行查询获取指定日期的轨迹数据
        cursor.execute(TRACK_QUERY, (date,))
        raw_data = cursor.fetchall()
        
        # 检查是否有足够的数据
        if not raw_data:
            return render_template('track_visualization.html', 
                                  date=date, 
                                  track_data=[], 
                                  error="该日期没有轨迹数据")
        
        # 处理时间戳格式，确保JSON序列化成功
        for point in raw_data:
            data_point = {}
            
            # 确保所有值都有默认值
            for key in point.keys():
                data_point[key] = point[key] if point[key] is not None else ""
                
            # 处理时间格式
            if 'create_time' in point and isinstance(point['create_time'], datetime):
                data_point['create_time'] = point['create_time'].strftime('%Y-%m-%d %H:%M:%S')
            
            # 确保经纬度是浮点数，并且不为零
            try:
                if 'lat' in point and point['lat'] is not None:
                    lat_value = float(point['lat'])
                    # 跳过无效值或零值
                    if lat_value == 0:
                        continue
                    data_point['lat'] = lat_value
                else:
                    # 如果没有有效的纬度，跳过此点
                    continue
                    
                if 'lon' in point and point['lon'] is not None:
                    lon_value = float(point['lon'])
                    # 跳过无效值或零值
                    if lon_value == 0:
                        continue
                    data_point['lon'] = lon_value
                else:
                    # 如果没有有效的经度，跳过此点
                    continue
                
                # 只有经纬度都有效的点才添加
                track_data.append(data_point)
            except (ValueError, TypeError):
                # 跳过无法转换为浮点数的点
                continue
        
        # 检查处理后是否有足够的数据点
        if len(track_data) < 2:
            return render_template('track_visualization.html', 
                                  date=date, 
                                  track_data=[], 
                                  error=f"该日期的轨迹数据不足，仅有{len(track_data)}个有效点")
        
        # 按时间排序
        track_data.sort(key=lambda x: x.get('create_time', ''))
        
    except mysql.connector.Error as err:
        error = f"数据库错误: {err}"
        app.logger.error(f"数据库错误: {err}")
    except Exception as e:
        error = f"发生意外错误: {e}"
        app.logger.error(f"意外错误: {e}")
    finally:
        # 关闭数据库连接
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    
    return render_template('track_visualization.html', date=date, track_data=track_data, error=error)

# --- 数据扩增功能 ---
@app.route('/augment_data', methods=['POST'])
def augment_data():
    """API端点 - 复制并调整指定时间段内的数据（优化版-使用分区表）"""
    try:
        data = request.get_json()
        original_start_time = data.get('original_start_time')
        original_end_time = data.get('original_end_time')
        time_delta_seconds = data.get('time_delta_seconds')
        date_str = data.get('date')
        
        if not all([original_start_time, original_end_time, time_delta_seconds is not None, date_str]):
            return jsonify({'success': False, 'error': '缺少必要参数'}), 400
            
        # 解析日期和时间
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            original_start_dt = datetime.strptime(f"{date_str} {original_start_time}", '%Y-%m-%d %H:%M:%S')
            original_end_dt = datetime.strptime(f"{date_str} {original_end_time}", '%Y-%m-%d %H:%M:%S')
            
            # 处理可能的跨天情况
            if original_end_dt < original_start_dt:
                original_end_dt += timedelta(days=1)
                
        except ValueError as e:
            return jsonify({'success': False, 'error': f'时间格式错误: {e}'}), 400
            
        # 处理需要增强的表名列表
        TABLES_TO_PROCESS = [
            "`信号灯感知表`",
            "`决策规划表`",
            "`定位表`",
            "`底盘表`",
            "`控制表`",
            "`激光雷达障碍物感知表`"
            # 可以根据实际需要添加更多表
        ]
        
        # 建立数据库连接
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # 开始事务
        conn.start_transaction()
        
        try:
            # 处理每个表
            for table_name in TABLES_TO_PROCESS:
                # 获取该表是否有分区（主要用于决策规划表）
                is_partitioned = False
                if table_name == "`决策规划表`":
                    cursor.execute("""
                        SELECT COUNT(*) as partition_count 
                        FROM information_schema.PARTITIONS 
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    """, (DB_CONFIG['database'], '决策规划表'))
                    result = cursor.fetchone()
                    is_partitioned = result and result['partition_count'] > 1
                
                # 优化：对于决策规划表，使用分区优化
                if is_partitioned and table_name == "`决策规划表`":
                    # 对于分区表，使用分批处理并直接使用SQL操作，避免大量数据在Python端处理
                    # 计算调整后的日期（新数据日期）
                    new_date = (original_start_dt + timedelta(seconds=time_delta_seconds)).date()
                    new_date_str = new_date.strftime('%Y-%m-%d')
                    
                    # 构建更高效的SQL复制语句（直接在数据库层面处理数据转换）
                    insert_sql = f"""
                    INSERT INTO {table_name}
                    SELECT 
                        vehicle_vin_code,
                        data_type,
                        %s as data_date, -- 使用新日期
                        DATE_ADD(create_time, INTERVAL %s SECOND) as create_time,
                        (CAST(timestamp AS DECIMAL(30,6)) + %s) as timestamp,
                        turn_light,
                        hazard_light,
                        p_id,
                        x, y, z,
                        o_x, o_y, o_z, o_w,
                        longitudinal_velocity_mps,
                        lateral_velocity_mps,
                        acceleration_mps2,
                        heading_rate_rps,
                        front_wheel_angle_rad
                    FROM {table_name}
                    WHERE create_time >= %s AND create_time <= %s
                      AND data_date = %s -- 使用分区裁剪
                    """
                    cursor.execute(insert_sql, (
                        new_date_str, 
                        time_delta_seconds, 
                        time_delta_seconds, 
                        original_start_dt, 
                        original_end_dt,
                        target_date
                    ))
                    print(f"执行分区优化插入决策规划表，影响行数: {cursor.rowcount}")
                else:
                    # 查询原始数据
                    query_sql = f"SELECT * FROM {table_name} WHERE create_time >= %s AND create_time <= %s"
                    cursor.execute(query_sql, (original_start_dt, original_end_dt))
                    original_records = cursor.fetchall()
                    
                    if not original_records:
                        continue  # 跳过没有数据的表
                        
                    # 准备插入的新数据
                    for record in original_records:
                        new_record = record.copy()
                        
                        # 调整时间字段
                        if 'create_time' in new_record and new_record['create_time']:
                            new_record['create_time'] = new_record['create_time'] + timedelta(seconds=time_delta_seconds)
                            
                        if 'data_date' in new_record and new_record['create_time']:
                            new_record['data_date'] = new_record['create_time'].date()
                            
                        if 'timestamp' in new_record and new_record['timestamp']:
                            try:
                                # 尝试将timestamp作为浮点数处理
                                original_ts = float(new_record['timestamp'])
                                new_ts = original_ts + time_delta_seconds
                                # 保持原始格式
                                if '.' in str(new_record['timestamp']):
                                    decimal_places = len(str(new_record['timestamp']).split('.')[-1])
                                    new_record['timestamp'] = f"{new_ts:.{decimal_places}f}"
                                else:
                                    new_record['timestamp'] = str(int(new_ts))
                            except (ValueError, TypeError) as e:
                                # 如果无法转换为浮点数，记录错误并保持不变
                                print(f"无法调整timestamp '{new_record['timestamp']}': {e}")
                                pass
                                
                        # 构建动态的INSERT语句
                        columns = list(new_record.keys())
                        placeholders = ["%s"] * len(columns)
                        column_str = ", ".join([f"`{col}`" for col in columns])
                        value_str = ", ".join(placeholders)
                        
                        # 执行INSERT IGNORE避免主键冲突
                        insert_sql = f"INSERT IGNORE INTO {table_name} ({column_str}) VALUES ({value_str})"
                        cursor.execute(insert_sql, list(new_record.values()))
            
            # 提交事务
            conn.commit()
            return jsonify({'success': True})
            
        except Exception as e:
            # 发生错误时回滚
            conn.rollback()
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

def get_original_segments(conn, target_date_str, gap_threshold=60):
    """获取指定日期的原始数据段落"""
    segments = []
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT create_time
            FROM 定位表
            WHERE data_date = %s
            ORDER BY create_time
        """, (target_date_str,))
        timestamps = [row['create_time'] for row in cursor.fetchall() if row['create_time']]

        if len(timestamps) > 1:
            segment_start_time = timestamps[0]
            segment_start_idx = 0
            for i in range(1, len(timestamps)):
                time_diff = (timestamps[i] - timestamps[i-1]).total_seconds()
                if time_diff > gap_threshold:
                    segment_end_time = timestamps[i-1]
                    segment_duration = (segment_end_time - segment_start_time).total_seconds()
                    if segment_duration > 0: # 忽略零长度段
                         segments.append({
                             'start': segment_start_time,
                             'end': segment_end_time,
                             'duration': segment_duration
                         })
                    segment_start_time = timestamps[i]
                    segment_start_idx = i

            # 处理最后一个段落
            segment_end_time = timestamps[-1]
            segment_duration = (segment_end_time - segment_start_time).total_seconds()
            if segment_duration > 0:
                 segments.append({
                     'start': segment_start_time,
                     'end': segment_end_time,
                     'duration': segment_duration
                 })
        elif len(timestamps) == 1:
             # 只有一个点，算作一个零长度段或忽略，这里选择忽略
             pass

    except mysql.connector.Error as err:
        print(f"获取段落时出错: {err}")
        # 可以抛出异常或返回空列表
        raise err # 让调用者处理
    finally:
        cursor.close()
    return segments

def generate_augmentation_plan(target_date, original_segments):
    """根据工作时间和原始段落生成扩增计划 (带随机性)"""
    plan = []
    if not original_segments:
        return plan

    # 定义基础工作时间段 (time objects)
    base_working_slots_times = [
        (time(6, 30, 0), time(11, 30, 0)),
        (time(13, 30, 0), time(18, 30, 0)),
        (time(19, 30, 0), time(22, 30, 0))
    ]

    # ---- 添加随机扰动到工作时间段 ----
    working_slots_dt = []
    for base_start, base_end in base_working_slots_times:
        # 计算随机偏移量 (秒)
        start_offset = random.randint(-15 * 60, 15 * 60)
        end_offset = random.randint(-15 * 60, 15 * 60)

        # 应用偏移量
        start_dt = datetime.combine(target_date, base_start) + timedelta(seconds=start_offset)
        end_dt = datetime.combine(target_date, base_end) + timedelta(seconds=end_offset)

        # 确保开始时间不晚于结束时间，并且至少有1分钟间隔
        if start_dt >= end_dt - timedelta(minutes=1):
            print(f"警告: 随机化后的时间段无效 ({start_dt.time()} - {end_dt.time()})，使用基础时间")
            start_dt = datetime.combine(target_date, base_start)
            end_dt = datetime.combine(target_date, base_end)
        else:
             print(f"应用随机化时间段: {start_dt.time()} - {end_dt.time()}")

        working_slots_dt.append((start_dt, end_dt))
    # ---- 结束时间段随机化 ----

    current_slot_index = 0
    segment_to_copy_index = 0

    if not working_slots_dt:
         return plan

    current_time_in_slot = working_slots_dt[current_slot_index][0]

    max_iterations = len(original_segments) * 50
    iterations = 0

    while current_slot_index < len(working_slots_dt) and iterations < max_iterations:
        iterations += 1
        slot_start, slot_end = working_slots_dt[current_slot_index]

        # 选择要复制的段落
        original_segment = original_segments[segment_to_copy_index]
        original_duration_seconds = original_segment['duration']
        original_segment_duration_td = timedelta(seconds=original_duration_seconds)

        # ---- 添加数据段随机裁剪 ----
        # 随机减少 0 到 5 分钟 (300 秒)
        trim_seconds = random.randint(0, 300)
        placement_duration_seconds = max(0, original_duration_seconds - trim_seconds) # 确保不为负
        placement_duration_td = timedelta(seconds=placement_duration_seconds)
        print(f"  选择段 {segment_to_copy_index+1}: 原始时长 {original_segment_duration_td}, 裁剪 {trim_seconds}s, 放置时长 {placement_duration_td}")
        # ---- 结束数据段随机裁剪 ----

        # 计算放置后的结束时间
        potential_end_time = current_time_in_slot + placement_duration_td # 使用裁剪后的时长计算

        # 检查新段落是否超出当前时间段
        if potential_end_time > slot_end:
            current_slot_index += 1
            if current_slot_index >= len(working_slots_dt):
                break
            current_time_in_slot = working_slots_dt[current_slot_index][0]
            slot_start, slot_end = working_slots_dt[current_slot_index]
            potential_end_time = current_time_in_slot + placement_duration_td # 重新计算
            if potential_end_time > slot_end:
                 print(f"警告：裁剪后段落 {segment_to_copy_index+1} (放置时长 {placement_duration_td}) 仍无法放入时间段 {current_slot_index+1}，跳过")
                 segment_to_copy_index = (segment_to_copy_index + 1) % len(original_segments)
                 continue

        # 如果可以放置
        new_start_time = current_time_in_slot
        original_start_dt = original_segment['start'] # 用于数据查询
        original_end_dt = original_segment['end']   # 用于数据查询

        time_delta_seconds = (new_start_time - original_start_dt).total_seconds()

        # 添加到计划 (注意：计划中仍使用原始段落信息进行数据查找)
        plan.append({
            'original_start_dt': original_start_dt,
            'original_end_dt': original_end_dt,
            'time_delta_seconds': time_delta_seconds
        })

        # ---- 添加随机间隔 ----
        random_gap_minutes = random.randint(7, 13)
        random_gap_td = timedelta(minutes=random_gap_minutes)
        print(f"  放置段落于 {new_start_time.strftime('%H:%M:%S')}, 下个间隔 {random_gap_td}")
        # ---- 结束随机间隔 ----

        # 更新下一次放置的开始时间（使用放置时长 + 随机间隔）
        current_time_in_slot = new_start_time + placement_duration_td + random_gap_td

        # 循环使用下一个段落
        segment_to_copy_index = (segment_to_copy_index + 1) % len(original_segments)

    print(f"生成带随机性的扩增计划，共 {len(plan)} 个复制操作")
    return plan


def execute_augmentation_plan(conn, plan, tables_to_process, batch_size=5000):
    """执行扩增计划 (支持分批处理)"""
    cursor = conn.cursor(dictionary=True)
    total_inserted_count = 0
    try:
        for idx, operation in enumerate(plan):
            original_start_dt = operation['original_start_dt']
            original_end_dt = operation['original_end_dt']
            time_delta_seconds = operation['time_delta_seconds']
            print(f"  执行操作 {idx+1}/{len(plan)}: Delta={time_delta_seconds}s, 源={original_start_dt.strftime('%H:%M:%S')}-{original_end_dt.strftime('%H:%M:%S')}")

            for table_name in tables_to_process:
                # 查询原始数据
                query_sql = f"SELECT * FROM {table_name} WHERE create_time >= %s AND create_time <= %s"
                cursor.execute(query_sql, (original_start_dt, original_end_dt))
                original_records = cursor.fetchall()

                if not original_records:
                    continue

                print(f"    表 {table_name}: 找到 {len(original_records)} 条原始记录")
                
                # 分批处理记录
                records_processed = 0
                current_batch = []
                
                # 准备SQL语句（只需准备一次）
                column_names = list(original_records[0].keys())
                column_clause = ", ".join([f"`{col}`" for col in columns])
                value_placeholders = ", ".join(["%s"] * len(column_names))
                insert_sql = f"INSERT IGNORE INTO {table_name} ({column_clause}) VALUES ({value_placeholders})"

                for record in original_records:
                    new_record_values_tuple = []
                    original_create_time = record.get('create_time')
                    original_timestamp_str = record.get('timestamp')

                    # 计算新时间
                    new_create_time = None
                    if isinstance(original_create_time, datetime):
                        new_create_time = original_create_time + timedelta(seconds=time_delta_seconds)
                    else: continue # 跳过无效create_time的记录

                    new_timestamp_str = None
                    if original_timestamp_str:
                        try:
                            original_ts_float = float(original_timestamp_str)
                            new_ts_float = original_ts_float + time_delta_seconds
                            if '.' in original_timestamp_str:
                                precision = len(original_timestamp_str.split('.')[-1])
                                new_timestamp_str = f"{new_ts_float:.{precision}f}"
                            else:
                                new_timestamp_str = str(math.trunc(new_ts_float)) # 使用trunc避免round引入的问题
                        except (ValueError, TypeError):
                            new_timestamp_str = original_timestamp_str # 保持原样

                    new_data_date = new_create_time.date()

                    # 构建新记录的值列表
                    for col in column_names:
                        if col == 'create_time':
                            new_record_values_tuple.append(new_create_time)
                        elif col == 'timestamp':
                            new_record_values_tuple.append(new_timestamp_str)
                        elif col == 'data_date':
                            new_record_values_tuple.append(new_data_date)
                        else:
                            new_record_values_tuple.append(record[col])

                    current_batch.append(tuple(new_record_values_tuple))
                    records_processed += 1
                    
                    # 当积累了一批记录或处理到最后一条记录时执行插入
                    if len(current_batch) >= batch_size or records_processed == len(original_records):
                        if current_batch:  # 确保有记录要插入
                            start_time = datetime.now()
                            cursor.executemany(insert_sql, current_batch)
                            batch_count = len(current_batch)
                            total_inserted_count += batch_count
                            elapsed = (datetime.now() - start_time).total_seconds()
                            print(f"      已插入批次: {batch_count} 条记录 ({elapsed:.2f}秒), 总计: {total_inserted_count}")
                            
                            # 为长操作提交当前批次，减少事务大小
                            if total_inserted_count % (batch_size * 10) == 0:  # 每10个批次提交一次
                                conn.commit()
                                print(f"      提交中间事务 (已处理 {total_inserted_count} 条记录)")
                                # 重新开始事务
                                conn.autocommit = False
                                
                            # 清空当前批次
                            current_batch = []

                print(f"    表 {table_name}: 完成 {records_processed} 条记录处理")

        print(f"扩增计划执行完毕，尝试插入约 {total_inserted_count} 条记录 (忽略重复)")
        return total_inserted_count

    except mysql.connector.Error as err:
        print(f"执行计划时出错: {err}")
        raise err # 让调用者处理回滚
    finally:
        cursor.close()

def execute_fast_augmentation(conn, plan, tables_to_process, sampling_rate=5):
    """
    快速执行扩增计划 - 使用SQL直接插入和数据抽样
    
    使用抽样和直接SQL插入大幅提高执行速度：
    1. 直接使用INSERT INTO SELECT语句代替获取记录后逐条处理
    2. 使用MOD(id, N)=0进行抽样，只处理部分数据
    3. 无需在内存中构建批量插入列表
    """
    cursor = conn.cursor()
    total_inserted_count = 0
    
    try:
        # 处理每个扩增操作
        for idx, operation in enumerate(plan):
            original_start_dt = operation['original_start_dt']
            original_end_dt = operation['original_end_dt']
            time_delta_seconds = operation['time_delta_seconds']
            
            print(f"  执行快速操作 {idx+1}/{len(plan)}: Delta={time_delta_seconds}s, 源={original_start_dt.strftime('%H:%M:%S')}-{original_end_dt.strftime('%H:%M:%S')}")
            
            # 处理每个表
            for table_name in tables_to_process:
                table_start_time = datetime.now()
                
                # 获取表格名称（不带反引号）
                clean_table_name = table_name.replace('`', '')
                
                # 获取表结构信息
                cursor.execute(f"DESCRIBE {table_name}")
                columns = cursor.fetchall()
                column_names = [col[0] for col in columns]
                
                # 检查是否有主键或唯一索引
                cursor.execute("""
                    SELECT COLUMN_NAME
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = %s
                    AND INDEX_NAME = 'PRIMARY'
                """, (clean_table_name,))
                primary_keys = [row[0] for row in cursor.fetchall()]
                
                # 如果找不到主键，尝试查找唯一索引作为替代
                if not primary_keys:
                    cursor.execute("""
                        SELECT COLUMN_NAME 
                        FROM information_schema.STATISTICS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = %s
                        AND NON_UNIQUE = 0
                    """, (clean_table_name,))
                    primary_keys = [row[0] for row in cursor.fetchall()]
                
                # 检查ID列的存在（用于抽样）
                has_id = 'id' in [col.lower() for col in column_names]
                
                # 构建抽样条件
                if has_id and sampling_rate > 1:
                    sampling_condition = f"AND MOD(id, {sampling_rate}) = 0"
                else:
                    sampling_condition = ""
                
                # 构建列表达式
                column_expressions = []
                for col in column_names:
                    col_lower = col.lower()
                    if col_lower == 'create_time':
                        column_expressions.append(f"DATE_ADD(`{col}`, INTERVAL {time_delta_seconds} SECOND) AS `{col}`")
                    elif col_lower == 'timestamp':
                        column_expressions.append(f"(CAST(`{col}` AS DECIMAL(30,6)) + {time_delta_seconds}) AS `{col}`")
                    elif col_lower == 'data_date':
                        column_expressions.append(f"DATE(DATE_ADD(`create_time`, INTERVAL {time_delta_seconds} SECOND)) AS `{col}`")
                    # 如果列在主键中，生成新值避免主键冲突
                    elif primary_keys and col in primary_keys:
                        # 获取列的数据类型
                        cursor.execute(f"SHOW COLUMNS FROM {table_name} WHERE Field = %s", (col,))
                        column_info = cursor.fetchone()
                        column_type = column_info[1].lower() if column_info else ""
                        
                        # 确保column_type是字符串类型
                        if isinstance(column_type, bytes):
                            column_type = column_type.decode('utf-8')
                        
                        # 根据列类型选择处理方式
                        if any(numeric_type in column_type for numeric_type in ['int', 'decimal', 'double', 'float', 'bit']):
                            # 数值类型，使用加法
                            column_expressions.append(f"(`{col}` + 10000000 + {idx*1000000}) AS `{col}`")
                        elif 'char' in column_type or 'text' in column_type or 'uuid' in column_type or 'binary' in column_type:
                            # 字符串/UUID类型，使用字符串修改
                            prefix = f"copy{idx}_"
                            column_expressions.append(f"CONCAT('{prefix}', `{col}`) AS `{col}`")
                        else:
                            # 其他类型，保持不变
                            column_expressions.append(f"`{col}`")
                    else:
                        column_expressions.append(f"`{col}`")
                
                # 对于决策规划表特殊处理 - 这是一个已知缓慢的表格
                is_decision_table = '决策规划表' in table_name
                if is_decision_table:
                    # 增加抽样率，对决策规划表更激进地抽样
                    actual_sampling_rate = sampling_rate * 2
                    if has_id:
                        sampling_condition = f"AND MOD(id, {actual_sampling_rate}) = 0"
                
                # 构建并执行INSERT查询
                column_clause = ", ".join(column_expressions)
                
                query = f"""
                INSERT INTO {table_name} 
                SELECT {column_clause}
                FROM {table_name} 
                WHERE create_time >= %s AND create_time <= %s
                {sampling_condition}
                """
                
                cursor.execute(query, (original_start_dt, original_end_dt))
                rows_affected = cursor.rowcount
                
                if rows_affected > 0:
                    total_inserted_count += rows_affected
                    elapsed = (datetime.now() - table_start_time).total_seconds()
                    print(f"    表 {table_name}: 快速插入 {rows_affected} 条记录 ({elapsed:.2f}秒)")
                else:
                    print(f"    表 {table_name}: 无符合条件的数据")
                
                # 中间提交，避免事务过大
                if total_inserted_count > 0 and total_inserted_count % 50000 == 0:
                    conn.commit()
                    print(f"    中间提交: 已处理 {total_inserted_count} 条记录")
                    conn.autocommit = False  # 重新开始事务
        
        print(f"快速扩增计划执行完毕，共插入 {total_inserted_count} 条记录")
        return total_inserted_count
        
    except mysql.connector.Error as err:
        print(f"快速执行计划时出错: {err}")
        raise err
    finally:
        cursor.close()

def execute_sql_batch_augmentation(conn, plan, tables_to_process):
    """
    超高性能SQL批量执行 - 直接通过SQL批量插入，无需Python循环处理每条记录
    
    为每个表和每个时间段创建一个INSERT语句，并直接在SQL层面完成所有时间转换
    大幅减少Python和数据库之间的交互，同时避免锁表问题
    """
    cursor = conn.cursor()
    total_inserted_count = 0
    
    try:
        # 处理每个表
        for table_name in tables_to_process:
            table_start_time = datetime.now()
            print(f"开始处理表 {table_name}")
            
            # 检查表是否是分区表（特别是决策规划表）
            is_partitioned = False
            if '决策规划表' in table_name:
                # 检查表是否有分区
                clean_table_name = table_name.replace('`', '')
                cursor.execute("""
                    SELECT COUNT(*) as cnt
                    FROM information_schema.PARTITIONS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = %s
                    GROUP BY TABLE_NAME
                """, (clean_table_name,))
                result = cursor.fetchone()
                is_partitioned = result and result[0] > 1
                if is_partitioned:
                    print(f"  检测到 {table_name} 是分区表，将使用分区优化")
            
            # 获取表结构信息
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            column_names = [col[0] for col in columns]
            column_names_lower = [col.lower() for col in column_names]
            
            # 检查表是否有id列（用于抽样）
            has_id_column = 'id' in column_names_lower
            
            # 检查主键信息
            clean_table_name = table_name.replace('`', '')
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
                AND INDEX_NAME = 'PRIMARY'
            """, (clean_table_name,))
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # 确定表的处理策略
            is_large_table = '决策规划表' in table_name
            rows_per_batch = 2000 if is_large_table else 5000
            
            # 为这个表创建一个合并的SQL语句，处理所有计划中的操作
            inserted_records = 0
            
            # 对于任何表，都使用分段处理以避免锁表问题
            for idx, operation in enumerate(plan):
                original_start_dt = operation['original_start_dt']
                original_end_dt = operation['original_end_dt']
                time_delta_seconds = operation['time_delta_seconds']
                
                # 计算新日期（用于分区优化）
                new_date = (original_start_dt + timedelta(seconds=time_delta_seconds)).date()
                new_date_str = new_date.strftime('%Y-%m-%d')
                
                # 原始日期（用于分区查询）
                original_date = original_start_dt.date()
                original_date_str = original_date.strftime('%Y-%m-%d')
                
                # 对于决策规划表的分区优化
                if is_partitioned and '决策规划表' in table_name:
                    partition_query = """
                    SELECT PARTITION_NAME
                    FROM information_schema.PARTITIONS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = %s
                    AND PARTITION_NAME LIKE %s
                    """
                    
                    # 查找原始日期对应的分区
                    cursor.execute(partition_query, (clean_table_name, f"p{original_date_str.replace('-', '')}%"))
                    src_partition = cursor.fetchone()
                    
                    # 查找目标日期对应的分区
                    cursor.execute(partition_query, (clean_table_name, f"p{new_date_str.replace('-', '')}%"))
                    dest_partition = cursor.fetchone()
                    
                    src_partition_name = src_partition[0] if src_partition else None
                    dest_partition_name = dest_partition[0] if dest_partition else None
                    
                    if src_partition_name and dest_partition_name:
                        print(f"  操作 {idx+1}/{len(plan)}: 使用分区优化 {src_partition_name} -> {dest_partition_name}")
                        
                        # 使用PARTITION子句直接指定分区
                        insert_query = f"""
                        REPLACE INTO {table_name} PARTITION({dest_partition_name})
                        SELECT 
                            vehicle_vin_code,
                            data_type,
                            '{new_date_str}' as data_date,
                            DATE_ADD(create_time, INTERVAL {time_delta_seconds} SECOND) as create_time,
                            (CAST(timestamp AS DECIMAL(30,9)) + {time_delta_seconds}) as timestamp,
                            turn_light,
                            hazard_light,
                            p_id,
                            x, y, z,
                            o_x, o_y, o_z, o_w,
                            longitudinal_velocity_mps,
                            lateral_velocity_mps,
                            acceleration_mps2,
                            heading_rate_rps,
                            front_wheel_angle_rad
                        FROM {table_name} PARTITION({src_partition_name})
                        WHERE create_time BETWEEN '{original_start_dt.strftime('%Y-%m-%d %H:%M:%S')}' AND '{original_end_dt.strftime('%Y-%m-%d %H:%M:%S')}'
                        """
                        
                        op_start_time = datetime.now()
                        cursor.execute(insert_query)
                        op_rows = cursor.rowcount
                        op_time = (datetime.now() - op_start_time).total_seconds()
                        
                        if op_rows > 0:
                            inserted_records += op_rows
                            print(f"  分区优化操作 {idx+1}/{len(plan)}: 插入/覆盖 {op_rows} 条记录 ({op_time:.2f}秒), Delta={time_delta_seconds}s")
                        
                        # 每操作提交一次，避免事务过大
                        conn.commit()
                        conn.autocommit = False
                    else:
                        print(f"  无法找到匹配的分区，降级为标准方式处理")
                        is_partitioned = False # 降级为标准方式
                
                if not (is_partitioned and '决策规划表' in table_name):
                    # 构建列表达式
                    column_expressions = []
                    for col in column_names:
                        col_lower = col.lower()
                        if col_lower == 'create_time':
                            column_expressions.append(f"DATE_ADD(`{col}`, INTERVAL {time_delta_seconds} SECOND) AS `{col}`")
                        elif col_lower == 'timestamp':
                            column_expressions.append(f"(CAST(`{col}` AS DECIMAL(30,6)) + {time_delta_seconds}) AS `{col}`")
                        elif col_lower == 'data_date':
                            column_expressions.append(f"'{new_date_str}' AS `{col}`")
                        # 对于主键/唯一索引列，先检查类型再选择处理方式
                        elif primary_keys and col in primary_keys:
                            # 获取列的数据类型
                            cursor.execute(f"SHOW COLUMNS FROM {table_name} WHERE Field = %s", (col,))
                            column_info = cursor.fetchone()
                            column_type = column_info[1].lower() if column_info else ""
                            
                            # 确保column_type是字符串类型
                            if isinstance(column_type, bytes):
                                column_type = column_type.decode('utf-8')
                            
                            # 根据列类型选择处理方式
                            if any(numeric_type in column_type for numeric_type in ['int', 'decimal', 'double', 'float', 'bit']):
                                # 数值类型，使用加法
                                column_expressions.append(f"(`{col}` + 10000000 + {idx*1000000}) AS `{col}`")
                            elif 'char' in column_type or 'text' in column_type or 'uuid' in column_type or 'binary' in column_type:
                                # 字符串/UUID类型，使用字符串修改
                                prefix = f"copy{idx}_"
                                column_expressions.append(f"CONCAT('{prefix}', `{col}`) AS `{col}`")
                            else:
                                # 其他类型，保持不变
                                column_expressions.append(f"`{col}`")
                        else:
                            column_expressions.append(f"`{col}`")
                    
                    # 构建列子句
                    column_clause = ", ".join(column_expressions)
                    
                    # 对于决策规划表，添加采样以减少数据量
                    sampling_clause = ""
                    if is_large_table:
                        # 对大表强制添加LIMIT限制
                        sampling_rate = 20  # 取1/20的数据
                        if has_id_column:
                            sampling_clause = f"AND MOD(id, {sampling_rate}) = 0"
                        limit_clause = f"LIMIT {rows_per_batch}"
                    else:
                        limit_clause = ""
                    
                    # 添加分区条件（如果是决策规划表）
                    data_date_clause = ""
                    if 'data_date' in column_names_lower and '决策规划表' in table_name:
                        data_date_clause = f"AND data_date = '{original_date_str}'"
                    
                    # 查询总记录数以确定分批处理
                    count_query = f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE create_time >= %s AND create_time <= %s
                    {data_date_clause}
                    {sampling_clause}
                    """
                    cursor.execute(count_query, (original_start_dt, original_end_dt))
                    total_rows = cursor.fetchone()[0]
                    
                    # 如果记录数太多，启用分批处理
                    if total_rows > rows_per_batch:
                        # 使用分批次插入
                        total_batches = (total_rows + rows_per_batch - 1) // rows_per_batch
                        print(f"  操作 {idx+1}/{len(plan)}: 数据量较大 ({total_rows} 条)，将分 {total_batches} 批处理")
                        
                        # 如果表有 ID 列，使用 ID 范围分批
                        if has_id_column:
                            # 获取 ID 范围
                            min_id_query = f"SELECT MIN(id) FROM {table_name} WHERE create_time >= %s AND create_time <= %s {data_date_clause}"
                            max_id_query = f"SELECT MAX(id) FROM {table_name} WHERE create_time >= %s AND create_time <= %s {data_date_clause}"
                            
                            cursor.execute(min_id_query, (original_start_dt, original_end_dt))
                            min_id = cursor.fetchone()[0] or 0
                            
                            cursor.execute(max_id_query, (original_start_dt, original_end_dt))
                            max_id = cursor.fetchone()[0] or 0
                            
                            if min_id is not None and max_id is not None and min_id < max_id:
                                # 按ID范围分批
                                batch_size = (max_id - min_id + 1) // total_batches + 1
                                
                                for batch in range(total_batches):
                                    batch_min_id = min_id + batch * batch_size
                                    batch_max_id = min(max_id, batch_min_id + batch_size - 1)
                                    
                                    batch_query = f"""
                                    REPLACE INTO {table_name} 
                                    SELECT {column_clause}
                                    FROM {table_name} 
                                    WHERE create_time >= %s AND create_time <= %s
                                    {data_date_clause}
                                    AND id BETWEEN {batch_min_id} AND {batch_max_id}
                                    {sampling_clause}
                                    """
                                    
                                    op_start_time = datetime.now()
                                    cursor.execute(batch_query, (original_start_dt, original_end_dt))
                                    batch_rows = cursor.rowcount
                                    op_time = (datetime.now() - op_start_time).total_seconds()
                                    
                                    if batch_rows > 0:
                                        inserted_records += batch_rows
                                        print(f"    批次 {batch+1}/{total_batches}: 插入/覆盖 {batch_rows} 条记录 ({op_time:.2f}秒)")
                                    
                                    # 每批次提交，避免锁表
                                    conn.commit()
                                    conn.autocommit = False
                            else:
                                # 如果无法获取ID范围，使用LIMIT OFFSET分批
                                for offset in range(0, total_rows, rows_per_batch):
                                    batch_query = f"""
                                    REPLACE INTO {table_name} 
                                    SELECT {column_clause}
                                    FROM {table_name} 
                                    WHERE create_time >= %s AND create_time <= %s
                                    {data_date_clause}
                                    {sampling_clause}
                                    LIMIT {rows_per_batch} OFFSET {offset}
                                    """
                                    
                                    op_start_time = datetime.now()
                                    cursor.execute(batch_query, (original_start_dt, original_end_dt))
                                    batch_rows = cursor.rowcount
                                    op_time = (datetime.now() - op_start_time).total_seconds()
                                    
                                    if batch_rows > 0:
                                        inserted_records += batch_rows
                                        print(f"    批次 OFFSET {offset}: 插入/覆盖 {batch_rows} 条记录 ({op_time:.2f}秒)")
                                    
                                    # 每批次提交，避免锁表
                                    conn.commit()
                                    conn.autocommit = False
                        else:
                            # 如果表没有ID列，使用LIMIT OFFSET分批
                            for offset in range(0, total_rows, rows_per_batch):
                                batch_query = f"""
                                REPLACE INTO {table_name} 
                                SELECT {column_clause}
                                FROM {table_name} 
                                WHERE create_time >= %s AND create_time <= %s
                                {data_date_clause}
                                {sampling_clause}
                                LIMIT {rows_per_batch} OFFSET {offset}
                                """
                                
                                op_start_time = datetime.now()
                                cursor.execute(batch_query, (original_start_dt, original_end_dt))
                                batch_rows = cursor.rowcount
                                op_time = (datetime.now() - op_start_time).total_seconds()
                                
                                if batch_rows > 0:
                                    inserted_records += batch_rows
                                    print(f"    批次 OFFSET {offset}: 插入/覆盖 {batch_rows} 条记录 ({op_time:.2f}秒)")
                                
                                # 每批次提交，避免锁表
                                conn.commit()
                                conn.autocommit = False
                    else:
                        # 数据量不大，直接处理
                        insert_query = f"""
                        REPLACE INTO {table_name} 
                        SELECT {column_clause}
                        FROM {table_name} 
                        WHERE create_time >= %s AND create_time <= %s
                        {data_date_clause}
                        {sampling_clause}
                        {limit_clause}
                        """
                        
                        op_start_time = datetime.now()
                        cursor.execute(insert_query, (original_start_dt, original_end_dt))
                        op_rows = cursor.rowcount
                        op_time = (datetime.now() - op_start_time).total_seconds()
                        
                        if op_rows > 0:
                            inserted_records += op_rows
                            print(f"  操作 {idx+1}/{len(plan)}: 插入/覆盖 {op_rows} 条记录 ({op_time:.2f}秒), Delta={time_delta_seconds}s")
                        
                        # 每个操作后提交，避免事务过大
                        conn.commit() 
                        conn.autocommit = False  # 重新开始事务
            
            total_inserted_count += inserted_records
            table_time = (datetime.now() - table_start_time).total_seconds()
            print(f"表 {table_name} 处理完成: 共插入/覆盖 {inserted_records} 条记录 (总用时: {table_time:.2f}秒)")
        
        print(f"SQL批量扩增完成，共插入/覆盖 {total_inserted_count} 条记录")
        return total_inserted_count
        
    except mysql.connector.Error as err:
        print(f"SQL批量执行错误: {err}")
        raise err
    finally:
        cursor.close()

@app.route('/api/auto_augment_data/<date_partition>', methods=['POST'])
def auto_augment_data(date_partition):
    """自动根据工作时间扩增数据"""
    conn = None  # 确保连接变量初始化
    
    try:
        # 接收请求参数
        data = request.get_json() or {}
        tables_to_process = data.get('tables', None)  # 可选参数，指定要处理的表格
        batch_size = data.get('batch_size', 5000)  # 可选参数，指定每批次处理的记录数
        fast_mode = data.get('fast_mode', False)  # 是否使用快速模式
        sql_batch_mode = data.get('sql_batch_mode', False)  # 是否使用SQL批量处理模式
        sampling_rate = data.get('sampling_rate', 5)  # 每N条记录取一条（抽样率）
        
        # 1. 解析日期
        target_date = datetime.strptime(date_partition, '%Y%m%d').date()
        target_date_str = target_date.strftime('%Y-%m-%d')
        print(f"开始自动扩增日期: {target_date_str}")

    except ValueError:
        return jsonify({'success': False, 'error': '无效的日期分区格式，请使用 YYYYMMDD'}), 400

    try:
        # 2. 连接数据库并获取原始段落
        # 使用一个新的连接，不要重用可能有问题的连接
        if conn is not None and hasattr(conn, 'is_connected') and conn.is_connected():
            try:
                print("关闭可能存在的旧连接")
                conn.close()
            except:
                pass
                
        # 添加connection配置项，禁用自动开始事务和设置自动提交
        db_config = DB_CONFIG.copy()
        # 确保使用默认设置，不会自动开启事务
        db_config.update({
            'autocommit': True,  # 设置为True，手动控制事务
            'raise_on_warnings': True,  # 让警告变为错误，便于调试
            'get_warnings': True  # 获取警告信息
        })
        
        conn = mysql.connector.connect(**db_config)
        print(f"连接已建立，配置: autocommit={db_config.get('autocommit', '未设置')}")
        
        # 查询连接状态
        cursor = conn.cursor()
        cursor.execute("SELECT @@autocommit, @@transaction_isolation, CONNECTION_ID()")
        conn_status = cursor.fetchone()
        cursor.close()
        if conn_status:
            print(f"MySQL连接状态: autocommit={conn_status[0]}, isolation={conn_status[1]}, connection_id={conn_status[2]}")
        
        # 设置连接参数，确保连接健康
        print("确保连接处于非事务状态")
        conn.autocommit = True  # 开启自动提交，避免事务冲突
        
        original_segments = get_original_segments(conn, target_date_str)
        if not original_segments:
            return jsonify({'success': False, 'error': f'日期 {target_date_str} 没有找到可用的原始数据段落'}), 404
        print(f"找到 {len(original_segments)} 个原始段落")

        # 3. 生成扩增计划
        augmentation_plan = generate_augmentation_plan(target_date, original_segments)
        if not augmentation_plan:
             return jsonify({'success': False, 'error': '未能生成有效的扩增计划 (可能是没有原始数据或工作时间段)'}), 500

        # 4. 执行扩增计划 (包含事务)
        # 默认处理所有表格，除非指定了特定表格
        DEFAULT_TABLES = [
            "`信号灯感知表`", "`决策规划表`", "`定位表`",
            "`底盘表`", "`控制表`", "`激光雷达障碍物感知表`"
        ]
        
        # 如果指定了表格，则只处理指定的表格
        if tables_to_process:
            # 确保表名格式正确（添加反引号）
            processed_tables = []
            for table in tables_to_process:
                if not table.startswith('`') and not table.endswith('`'):
                    processed_tables.append(f"`{table}`")
                else:
                    processed_tables.append(table)
            
            TABLES_TO_PROCESS = processed_tables
            print(f"将只处理指定的 {len(TABLES_TO_PROCESS)} 个表: {', '.join(TABLES_TO_PROCESS)}")
        else:
            TABLES_TO_PROCESS = DEFAULT_TABLES
            print(f"处理所有 {len(TABLES_TO_PROCESS)} 个表")
        
        # 根据指定的模式选择执行方法
        if sql_batch_mode:
            # 超高性能SQL批量处理模式（推荐用于大多数情况）
            print("使用SQL批量处理模式 - 推荐高性能模式")
            try:
                conn.autocommit = False
                start_time = datetime.now()
                inserted_count = execute_sql_batch_augmentation(conn, augmentation_plan, TABLES_TO_PROCESS)
                elapsed_time = (datetime.now() - start_time).total_seconds()
                print("SQL批量模式数据处理已完成")
                conn.autocommit = True
                
                return jsonify({
                    'success': True,
                    'message': f'日期 {target_date_str} 数据SQL批量扩增完成。执行了 {len(augmentation_plan)} 个复制操作，插入 {inserted_count} 条记录 (耗时：{elapsed_time:.2f}秒)。',
                    'processed_tables': TABLES_TO_PROCESS,
                    'mode': 'sql_batch',
                    'elapsed_seconds': elapsed_time
                })
            except mysql.connector.Error as tx_err:
                print(f"SQL批量模式错误: {tx_err}")
                if conn and hasattr(conn, 'is_connected') and conn.is_connected():
                    try:
                        conn.rollback()
                        print("事务已回滚")
                    except Exception as rollback_err:
                        print(f"回滚时出错: {rollback_err}")
                raise tx_err
        elif fast_mode:
            print(f"使用快速模式处理数据 (抽样率 1:{sampling_rate})")
            # 快速模式处理
            try:
                conn.autocommit = False
                start_time = datetime.now()
                inserted_count = execute_fast_augmentation(conn, augmentation_plan, TABLES_TO_PROCESS, sampling_rate)
                elapsed_time = (datetime.now() - start_time).total_seconds()
                conn.commit()
                print("快速模式事务提交成功")
                conn.autocommit = True
                
                return jsonify({
                    'success': True,
                    'message': f'日期 {target_date_str} 数据快速扩增完成。执行了 {len(augmentation_plan)} 个复制操作，插入约 {inserted_count} 条记录 (使用1:{sampling_rate}抽样)。',
                    'processed_tables': TABLES_TO_PROCESS,
                    'mode': 'fast',
                    'elapsed_seconds': elapsed_time
                })
            except mysql.connector.Error as tx_err:
                print(f"快速模式事务操作错误: {tx_err}")
                if conn and hasattr(conn, 'is_connected') and conn.is_connected():
                    try:
                        conn.rollback()
                        print("事务已回滚")
                    except Exception as rollback_err:
                        print(f"回滚时出错: {rollback_err}")
                raise tx_err
        else:
            # 常规模式处理
            # 确保没有活跃事务
            try:
                print("检查事务状态并尝试开始新事务")
                
                # 确保已经没有活跃事务
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT @@autocommit")
                autocommit_status = cursor.fetchone()[0]
                cursor.close()
                print(f"当前自动提交状态: {autocommit_status}")
                
                # 现在可以安全地开始事务
                print("禁用自动提交并开始事务")
                conn.autocommit = False
                            
                # 执行一个查询，确认可以正常使用连接
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                print(f"测试查询结果: {result}")
                
                print("开始执行扩增计划事务")
                # 使用标准分批处理方法
                start_time = datetime.now()
                inserted_count = execute_augmentation_plan(conn, augmentation_plan, TABLES_TO_PROCESS, batch_size)
                elapsed_time = (datetime.now() - start_time).total_seconds()
                
                print("提交事务")
                conn.commit()
                print("事务提交成功")
                
                print("重新启用自动提交")
                conn.autocommit = True
                
                return jsonify({
                    'success': True,
                    'message': f'日期 {target_date_str} 数据自动扩增完成。执行了 {len(augmentation_plan)} 个复制操作，尝试插入约 {inserted_count} 条记录 (忽略重复)。',
                    'processed_tables': TABLES_TO_PROCESS,
                    'mode': 'standard',
                    'elapsed_seconds': elapsed_time
                })
            except mysql.connector.Error as tx_err:
                print(f"事务操作错误: {tx_err}")
                if conn and hasattr(conn, 'is_connected') and conn.is_connected():
                    try:
                        conn.rollback()
                        print("事务已回滚")
                    except Exception as rollback_err:
                        print(f"回滚时出错: {rollback_err}")
                raise tx_err  # 继续向上传播错误

    except mysql.connector.Error as err:
        if conn and conn.in_transaction:
            conn.rollback()
            print("数据库错误，事务已回滚")
        app.logger.error(f"数据库错误详细信息: {err}", exc_info=True)
        print(f"错误类型: {type(err).__name__}")
        print(f"错误代码: {getattr(err, 'errno', 'N/A')}")
        print(f"错误消息: {str(err)}")
        print(f"SQL状态: {getattr(err, 'sqlstate', 'N/A')}")
        return jsonify({'success': False, 'error': f'数据库操作失败: {err}', 'details': {
            'error_type': type(err).__name__,
            'error_code': getattr(err, 'errno', 'N/A'),
            'sql_state': getattr(err, 'sqlstate', 'N/A')
        }}), 500
    except Exception as e:
        if conn and conn.in_transaction:
            conn.rollback()
            print(f"未知错误 {type(e).__name__}，事务已回滚")
        app.logger.error(f"Auto augment error for {date_partition}: {e}", exc_info=True)
        print(f"详细错误信息: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': f'服务器内部错误: {e}', 'details': str(e)}), 500
    finally:
        if conn:
            try:
                # 检查连接状态
                if hasattr(conn, 'is_connected') and conn.is_connected():
                    print("关闭数据库连接...")
                    # 如果有事务未完成，尝试回滚
                    if hasattr(conn, 'in_transaction') and conn.in_transaction:
                        try:
                            print("发现未完成事务，尝试回滚")
                            conn.rollback()
                            print("事务回滚成功")
                        except Exception as rollback_err:
                            print(f"回滚未完成事务时出错: {rollback_err}")
                    
                    # 关闭连接
                    conn.close()
                    print("数据库连接关闭成功")
                else:
                    print("数据库连接已不可用，无需关闭")
            except Exception as close_err:
                print(f"关闭数据库连接时发生错误: {close_err}")
                import traceback
                traceback.print_exc()

# --- 新增：数据覆盖接口 (重构版) ---
@app.route('/api/overwrite_date/<target_date_str>', methods=['POST'])
def overwrite_date_data(target_date_str):
    """
    API端点 - 使用源日期数据，根据目标时长要求，填充覆盖目标日期数据。
    接收 POST 请求，JSON body 包含: {
        "source_date": "YYYY-MM-DD",
        "target_duration_hours": 6.0
    }
    """
    conn = None
    try:
        # 1. 获取参数
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': '请求体不能为空'}), 400
        source_date_str = data.get('source_date')
        target_duration_hours = data.get('target_duration_hours')

        if not source_date_str:
            return jsonify({'success': False, 'error': '缺少 source_date 参数'}), 400
        if target_duration_hours is None:
             return jsonify({'success': False, 'error': '缺少 target_duration_hours 参数'}), 400
        try:
            target_duration_hours = float(target_duration_hours)
            if target_duration_hours <= 0:
                raise ValueError("Target duration must be positive")
        except ValueError:
             return jsonify({'success': False, 'error': 'target_duration_hours 必须是一个正数'}), 400


        # 2. 解析日期
        try:
            target_date = datetime.strptime(target_date_str, '%Y%m%d').date()
            source_date = datetime.strptime(source_date_str, '%Y-%m-%d').date()
            print(f"请求覆盖数据: 源 {source_date} -> 目标 {target_date}, 期望时长 {target_duration_hours}h")
        except ValueError:
            return jsonify({'success': False, 'error': '无效的日期格式。目标日期请使用 YYYYMMDD，源日期请使用 YYYY-MM-DD'}), 400

        # 3. 连接数据库
        conn = mysql.connector.connect(**DB_CONFIG)
        # 显式设置 autocommit=True 清理连接状态
        conn.autocommit = True

        # 4. 获取源数据段落 (复用 get_original_segments)
        print(f"从源日期 {source_date_str} 获取数据段落...")
        source_segments = get_original_segments(conn, source_date_str)
        if not source_segments:
             conn.close()
             return jsonify({'success': False, 'error': f'源日期 {source_date_str} 没有找到可用的数据段落'}), 404
        print(f"找到 {len(source_segments)} 个源数据段落。")

        # 5. 生成覆盖计划
        print(f"为目标日期 {target_date} 生成覆盖计划 (目标时长 {target_duration_hours}h)...")
        overwrite_plan = generate_overwrite_plan(target_date, source_segments, target_duration_hours)
        if not overwrite_plan:
             conn.close()
             return jsonify({'success': False, 'error': '未能生成有效的覆盖计划'}), 500

        # 6. 定义需要处理的表
        TABLES_TO_PROCESS = [
            "`定位表`", "`决策规划表`", "`信号灯感知表`",
            "`底盘表`", "`控制表`", "`激光雷达障碍物感知表`"
        ]

        # 7. 执行覆盖操作 (删除 + 插入)
        cursor = conn.cursor(dictionary=True)
        total_inserted_rows = 0
        operation_start_time = datetime.now()

        try:
            # -- 先执行删除 --
            print(f"开始删除目标日期 {target_date} 的现有数据...")
            for table_name in TABLES_TO_PROCESS:
                 delete_sql = f"DELETE FROM {table_name} WHERE data_date = %s"
                 cursor.execute(delete_sql, (target_date,))
                 print(f"  表 {table_name}: 删除了 {cursor.rowcount} 行。")
            print("目标日期数据删除完成。")

            # -- 再执行插入 --
            print(f"开始根据计划执行插入操作 ({len(overwrite_plan)} 个步骤)...")
            # 设置 autocommit=False 准备手动事务
            conn.autocommit = False
            conn.start_transaction()

            for idx, operation in enumerate(overwrite_plan):
                original_start_dt = operation['original_start_dt']
                original_end_dt = operation['original_end_dt']
                time_delta_seconds = operation['time_delta_seconds']
                # print(f"  执行计划步骤 {idx+1}/{len(overwrite_plan)}: Delta={time_delta_seconds:.2f}s, 源={original_start_dt.strftime('%H:%M:%S')}-{original_end_dt.strftime('%H:%M:%S')}")

                for table_name in TABLES_TO_PROCESS:
                    # 7.1 获取表结构
                    cursor.execute(f"DESCRIBE {table_name}")
                    columns_info = cursor.fetchall()
                    column_names = [col['Field'] for col in columns_info]
                    column_names_quoted = [f"`{col}`" for col in column_names]
                    has_auto_increment_id = any(col['Field'].lower() == 'id' and 'auto_increment' in col.get('Extra', '').lower() for col in columns_info)

                    # 7.2 构建 INSERT INTO ... SELECT 语句
                    select_expressions = []
                    for col in column_names:
                        col_lower = col.lower()
                        if col_lower == 'id' and has_auto_increment_id:
                            select_expressions.append("NULL")
                        elif col_lower == 'data_date':
                            select_expressions.append(f"%s AS `data_date`") # 目标日期
                        elif col_lower == 'create_time':
                            select_expressions.append(f"DATE_ADD(`{col}`, INTERVAL %s SECOND) AS `{col}`")
                        elif col_lower == 'timestamp':
                            select_expressions.append(f"(CAST(`{col}` AS DECIMAL(30,6)) + %s) AS `{col}`")
                        else:
                            select_expressions.append(f"`{col}`")

                    select_clause = ", ".join(select_expressions)
                    column_clause = ", ".join(column_names_quoted)

                    insert_sql = f"""
                    INSERT INTO {table_name} ({column_clause})
                    SELECT {select_clause}
                    FROM {table_name}
                    WHERE create_time >= %s AND create_time <= %s
                      AND data_date = %s -- 从源日期查询
                    """

                    # 7.3 准备参数并执行
                    params = []
                    for expr in select_expressions:
                         if '%s AS `data_date`' in expr: params.append(target_date)
                         elif 'INTERVAL %s SECOND' in expr: params.append(time_delta_seconds)
                         elif '+ %s)' in expr: params.append(time_delta_seconds)
                    # WHERE 参数
                    params.append(original_start_dt)
                    params.append(original_end_dt)
                    params.append(source_date)

                    try:
                        cursor.execute(insert_sql, tuple(params))
                        inserted_rows = cursor.rowcount
                        total_inserted_rows += inserted_rows
                        # print(f"    表 {table_name}: 插入了 {inserted_rows} 行。")
                    except mysql.connector.Error as insert_err:
                        # 捕捉可能的插入错误，例如重复键（理论上不应发生，因为先删除了）
                        print(f"警告: 表 {table_name} 插入时发生错误 (步骤 {idx+1}): {insert_err}")
                        # 可以选择继续处理其他表或停止
                        pass # 暂时忽略并继续

            # 8. 提交事务
            conn.commit()
            elapsed_time = (datetime.now() - operation_start_time).total_seconds()
            print(f"目标日期 {target_date_str} 数据覆盖完成。共执行 {len(overwrite_plan)} 个复制操作，总计插入 {total_inserted_rows} 条记录 (耗时: {elapsed_time:.2f} 秒)。")
            return jsonify({
                'success': True,
                'message': f'成功使用 {source_date_str} 的数据覆盖了 {target_date_str} (目标时长 {target_duration_hours}h)。',
                'target_date': target_date_str,
                'source_date': source_date_str,
                'plan_steps': len(overwrite_plan),
                'total_rows_inserted': total_inserted_rows,
                'elapsed_seconds': elapsed_time
            })

        except mysql.connector.Error as err:
            print(f"数据库事务操作错误: {err}")
            if conn.in_transaction:
                 print("回滚事务...")
                 conn.rollback()
            raise err # 向上抛出以触发外层except
        except Exception as inner_e:
             print(f"执行计划时发生未知错误: {inner_e}")
             if conn.in_transaction:
                 print("回滚事务...")
                 conn.rollback()
             raise inner_e

    except mysql.connector.Error as err:
        app.logger.error(f"数据库错误 (overwrite_date_data): {err}", exc_info=True)
        return jsonify({'success': False, 'error': f'数据库操作失败: {err}'}), 500
    except Exception as e:
        app.logger.error(f"覆盖数据时发生未知错误 for {target_date_str}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'服务器内部错误: {e}'}), 500
    finally:
        if conn and conn.is_connected():
            # 确保最终将 autocommit 恢复为 True
            try:
                conn.autocommit = True
            except: pass
            cursor.close()
            conn.close()
            print("数据库连接已关闭。")


# --- 数据删除功能 ---
@app.route('/delete_data', methods=['POST'])
def delete_data():
    """API端点 - 删除指定时间段内的数据"""
    try:
        data = request.get_json()
        start_time = data.get('start_time')
        end_time = data.get('end_time')
        date_str = data.get('date')
        
        if not all([start_time, end_time, date_str]):
            return jsonify({'success': False, 'error': '缺少必要参数'}), 400
            
        # 解析日期和时间
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            start_dt = datetime.strptime(f"{date_str} {start_time}", '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(f"{date_str} {end_time}", '%Y-%m-%d %H:%M:%S')
            
            # 处理可能的跨天情况
            if end_dt < start_dt:
                end_dt += timedelta(days=1)
                
        except ValueError as e:
            return jsonify({'success': False, 'error': f'时间格式错误: {e}'}), 400
            
        # 处理需要删除数据的表名列表
        TABLES_TO_PROCESS = [
            "`信号灯感知表`",
            "`决策规划表`",
            "`定位表`",
            "`底盘表`",
            "`控制表`",
            "`激光雷达障碍物感知表`"
            # 可以根据实际需要添加更多表
        ]
        
        # 建立数据库连接
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 开始事务
        conn.start_transaction()
        
        try:
            # 处理每个表
            for table_name in TABLES_TO_PROCESS:
                # 删除指定时间段内的数据
                delete_sql = f"DELETE FROM {table_name} WHERE create_time >= %s AND create_time <= %s"
                cursor.execute(delete_sql, (start_dt, end_dt))
            
            # 提交事务
            conn.commit()
            return jsonify({'success': True})
            
        except Exception as e:
            # 发生错误时回滚
            conn.rollback()
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

def generate_overwrite_plan(target_date, source_segments, target_duration_hours):
    """根据目标时长、工作时间和源段落生成覆盖计划 (带随机性)

    Args:
        target_date (date): 目标日期.
        source_segments (list): 从源日期获取的段落列表，每个段落包含 'start', 'end', 'duration'.
        target_duration_hours (float): 目标日期期望的总数据时长（小时）.

    Returns:
        list: 覆盖计划，每个元素包含 'original_start_dt', 'original_end_dt', 'time_delta_seconds'.
              返回空列表表示无法生成计划。
    """
    plan = []
    if not source_segments:
        print("错误：源数据没有有效段落，无法生成覆盖计划。")
        return plan

    # 1. 计算带随机扰动的目标总时长 (秒)
    target_duration_seconds = target_duration_hours * 3600
    randomized_target_duration_seconds = target_duration_seconds * random.uniform(0.8, 1.2)
    print(f"目标时长: {target_duration_hours:.2f} 小时, 随机化后目标时长: {format_duration_seconds(randomized_target_duration_seconds)}")

    # 2. 定义和随机化工作时间段
    #    早上: 7:30 ± 30min 到 11:30
    #    下午: 13:30 到 结束时间 (根据总时长调整)
    morning_start_offset = random.randint(-30 * 60, 30 * 60)
    morning_start_dt = datetime.combine(target_date, time(7, 30, 0)) + timedelta(seconds=morning_start_offset)
    morning_end_dt = datetime.combine(target_date, time(11, 30, 0))

    # 如果随机开始时间晚于固定结束时间，则修正开始时间
    if morning_start_dt >= morning_end_dt:
        morning_start_dt = datetime.combine(target_date, time(7, 0, 0)) # 保守设为 7:00
        print(f"警告: 随机化后的早上班时间晚于早下班时间，已重置为 {morning_start_dt.time()}")

    morning_duration_seconds = (morning_end_dt - morning_start_dt).total_seconds()
    if morning_duration_seconds < 0:
        morning_duration_seconds = 0 # 避免负时长

    afternoon_start_dt = datetime.combine(target_date, time(13, 30, 0))

    # 计算下午需要填充的时长
    required_afternoon_seconds = max(0, randomized_target_duration_seconds - morning_duration_seconds)
    afternoon_end_dt = afternoon_start_dt + timedelta(seconds=required_afternoon_seconds)

    working_slots = [
        (morning_start_dt, morning_end_dt),
        (afternoon_start_dt, afternoon_end_dt)
    ]
    print(f"生成的工作时间段:")
    print(f"  - 早上: {working_slots[0][0].strftime('%H:%M:%S')} - {working_slots[0][1].strftime('%H:%M:%S')}")
    print(f"  - 下午: {working_slots[1][0].strftime('%H:%M:%S')} - {working_slots[1][1].strftime('%H:%M:%S')}")

    # 3. 循环填充段落直到达到目标时长
    current_slot_index = 0
    segment_to_copy_index = 0
    accumulated_duration_seconds = 0
    max_iterations = len(source_segments) * 100 # 防止无限循环
    iterations = 0

    if not working_slots or working_slots[current_slot_index][0] >= working_slots[current_slot_index][1]:
        print("错误：无效的工作时间段。")
        return []

    current_time_in_slot = working_slots[current_slot_index][0]

    while accumulated_duration_seconds < randomized_target_duration_seconds and current_slot_index < len(working_slots) and iterations < max_iterations:
        iterations += 1
        slot_start, slot_end = working_slots[current_slot_index]

        # 确保当前时间在有效槽内
        if current_time_in_slot < slot_start:
            current_time_in_slot = slot_start
        if current_time_in_slot >= slot_end:
             current_slot_index += 1
             if current_slot_index >= len(working_slots):
                 print("已用完所有工作时间段。")
                 break
             slot_start, slot_end = working_slots[current_slot_index]
             # 如果新槽无效，则退出
             if slot_start >= slot_end:
                 print(f"错误：下一个工作时间段 {current_slot_index+1} 无效。")
                 break
             current_time_in_slot = slot_start
             continue

        # 选择源段落 (循环使用)
        original_segment = source_segments[segment_to_copy_index]
        original_duration_seconds = original_segment['duration']

        # 随机裁剪 (0-5 分钟)
        trim_seconds = random.randint(0, 5 * 60)
        placement_duration_seconds = max(1, original_duration_seconds - trim_seconds) # 至少1秒
        placement_duration_td = timedelta(seconds=placement_duration_seconds)

        # 计算潜在结束时间
        potential_end_time = current_time_in_slot + placement_duration_td

        # 检查是否能放入当前时间段
        if potential_end_time > slot_end:
            # 当前段放不下了，换下一个时间段
            print(f"  段 {segment_to_copy_index+1} (裁剪后 {placement_duration_td}) 无法放入当前时间段 {current_slot_index+1} ({current_time_in_slot.strftime('%H:%M:%S')} -> {slot_end.strftime('%H:%M:%S')})，尝试下一个时间段")
            current_slot_index += 1
            if current_slot_index >= len(working_slots):
                print("已用完所有工作时间段。")
                break # 所有时间段都用完了
            # 更新到下一个时间段的开始
            slot_start, slot_end = working_slots[current_slot_index]
             # 如果新槽无效，则退出
            if slot_start >= slot_end:
                print(f"错误：下一个工作时间段 {current_slot_index+1} 无效。")
                break
            current_time_in_slot = slot_start
            continue # 返回循环开始处重新检查

        # 可以放置
        new_start_time = current_time_in_slot
        original_start_dt = original_segment['start']
        original_end_dt = original_segment['end']

        time_delta_seconds = (new_start_time - original_start_dt).total_seconds()

        # 添加到计划
        plan.append({
            'original_start_dt': original_start_dt,
            'original_end_dt': original_end_dt,
            'time_delta_seconds': time_delta_seconds
        })
        print(f"  计划操作 {len(plan)}: 复制源 {original_start_dt.strftime('%H:%M:%S')}-{original_end_dt.strftime('%H:%M:%S')} 到目标 {new_start_time.strftime('%H:%M:%S')}")

        # 更新累计时长
        accumulated_duration_seconds += placement_duration_seconds

        # 添加随机间隔 (7-13 分钟)
        random_gap_minutes = random.randint(7, 13)
        random_gap_td = timedelta(minutes=random_gap_minutes)

        # 更新下一次放置的开始时间
        current_time_in_slot = new_start_time + placement_duration_td + random_gap_td
        print(f"    累计时长: {format_duration_seconds(accumulated_duration_seconds)}, 下次放置时间: {current_time_in_slot.strftime('%H:%M:%S')}")

        # 换下一个源段落 (循环)
        segment_to_copy_index = (segment_to_copy_index + 1) % len(source_segments)

    if iterations >= max_iterations:
        print("警告：达到最大迭代次数，计划可能未完全达到目标时长。")

    print(f"生成覆盖计划完成，共 {len(plan)} 个复制操作，累计时长约 {format_duration_seconds(accumulated_duration_seconds)}")
    return plan

if __name__ == '__main__':
    # Note: Running on port 90 might require root privileges (e.g., using sudo)
    # Use a different port (like 5000) if you don't have/want root access
    try:
        app.run(host='0.0.0.0', port=90, debug=False) # Set debug=False for production
    except PermissionError:
        print("\nError: Permission denied to use port 90.")
        print("Try running with 'sudo python app.py' or use a port > 1024 (e.g., 5000).\n")
    except Exception as e:
         print(f"\nFailed to start Flask server: {e}\n")

