# -*- coding: utf-8 -*-
"""
封装跨站点操作的具体执行逻辑 (比较数量, 复制字段, 比较字段)。
"""
import copy
import json
import logging
from pathlib import Path
import asyncio
from asyncio import Semaphore
from typing import TYPE_CHECKING, List, Dict, Any, Optional # 用于类型提示

# 从项目模块导入
from channel_manager_lib.undo_utils import save_undo_data
from oneapi_tool_utils.data_helpers import normalize_to_set, normalize_to_dict # 导入辅助函数

if TYPE_CHECKING:
    # 避免循环导入，仅用于类型提示
    from oneapi_tool_utils.channel_tool_base import ChannelToolBase
    import argparse # 用于 args 的类型提示


# 定义字段类型常量 (从 handler 移过来)
LIST_FIELDS = ["models", "group", "tag"]
DICT_FIELDS = ["model_mapping", "status_code_mapping", "setting", "headers", "override_params"]

def execute_compare_channel_counts(
    source_channels_all: Optional[List[Dict[str, Any]]],
    target_channels_all: Optional[List[Dict[str, Any]]],
    source_config_path: Path,
    target_config_path: Path
) -> None:
    """
    比较并打印源和目标站点的渠道总数。

    Args:
        source_channels_all: 源站点的所有渠道列表 (或 None 如果获取失败)。
        target_channels_all: 目标站点的所有渠道列表 (或 None 如果获取失败)。
        source_config_path: 源连接配置文件的路径。
        target_config_path: 目标连接配置文件的路径。
    """
    print("\n--- 渠道数量比较结果 ---")
    source_count: Any = len(source_channels_all) if source_channels_all is not None else "获取失败"
    target_count: Any = len(target_channels_all) if target_channels_all is not None else "获取失败"
    print(f"源站点 ({source_config_path.name}): {source_count} 个渠道")
    print(f"目标站点 ({target_config_path.name}): {target_count} 个渠道")
    if isinstance(source_count, int) and isinstance(target_count, int):
        if source_count == target_count:
            print("渠道数量相同。")
        else:
            print(f"渠道数量不同 (相差 {abs(source_count - target_count)} 个)。")
    print("------------------------")
async def execute_copy_fields(
    args: 'argparse.Namespace',
    source_tool: 'ChannelToolBase',
    target_tool: 'ChannelToolBase',
    source_channel_data: Dict[str, Any],
    matched_target_channels: List[Dict[str, Any]],
    fields_to_copy: List[str],
    copy_mode: str,
    script_config: Dict[str, Any],
    target_config_path: Path # 需要用于保存撤销数据
) -> int:
    """
    执行复制字段的核心逻辑：准备计划、模拟、确认、并发更新、保存撤销、报告结果。

    Args:
        args: 解析后的命令行参数对象 (需要 .yes).
        source_tool: 源站点工具实例。
        target_tool: 目标站点工具实例。
        source_channel_data: 已确定的单个源渠道数据字典。
        matched_target_channels: 筛选出的目标渠道列表。
        fields_to_copy: 需要复制的字段名列表。
        copy_mode: 复制模式 ("overwrite", "append", "remove", "merge", "delete_keys")。
        script_config: 已加载的脚本通用配置 (需要 concurrency_limit)。
        target_config_path: 目标连接配置文件的路径 (用于撤销)。

    Returns:
        int: 退出码 (0 表示成功, 1 表示有失败)。
    """
    # --- 复制字段逻辑 (从 cross_site_handler 移入) ---
    # 1. 准备更新计划
    update_plan = []
    source_data_to_copy = {field: source_channel_data.get(field) for field in fields_to_copy if field not in ['id', 'key']}

    logging.info(f"将使用源渠道 ID: {source_channel_data.get('id')}, Name: '{source_channel_data.get('name')}' 的数据进行复制。")
    logging.info(f"准备对 {len(matched_target_channels)} 个匹配的目标渠道计算更新计划...")

    # 预先标准化源数据（用于列表和字典）
    normalized_source_data = {}
    source_name_for_log = source_channel_data.get('name', f"ID:{source_channel_data.get('id')}")
    try:
        for field, value in source_data_to_copy.items():
            if field in LIST_FIELDS:
                normalized_source_data[field] = normalize_to_set(value)
            elif field in DICT_FIELDS:
                normalized_source_data[field] = normalize_to_dict(value, field, source_name_for_log)
            # else: 简单类型不需要预标准化
    except Exception as norm_e:
        logging.error(f"标准化源渠道字段时出错: {norm_e}", exc_info=True)
        print(f"错误：标准化源渠道数据时出错，无法准备更新计划。")
        return 1
    except Exception as norm_e:
        logging.error(f"标准化源渠道字段时出错: {norm_e}", exc_info=True)
        print(f"错误：标准化源渠道数据时出错，无法准备更新计划。")
        return 1

    # 遍历每个目标渠道，计算变更
    for target_channel in matched_target_channels:
        target_id = target_channel.get('id')
        target_name = target_channel.get('name')
        target_name_for_log = target_name or f"ID:{target_id}"
        logging.debug(f"开始为目标渠道 ID: {target_id}, Name: '{target_name}' 准备更新...")

        changes_summary = {} # 记录人类可读的变更
        payload_for_api = {'id': target_id} # API 更新通常只需要 ID 和变化的字段
        field_processing_errors = False # 标记此渠道是否有字段处理错误

        for field in fields_to_copy:
            if field in ['id', 'key']: continue # 再次确认跳过

            source_value = source_data_to_copy.get(field) # 原始源值，用于 delete_keys 等模式判断
            original_target_value = target_channel.get(field)
            field_changed = False
            new_value = original_target_value # 默认不改变

            try:
                # --- 列表字段处理 ---
                if field in LIST_FIELDS:
                    current_target_set = normalize_to_set(original_target_value)
                    source_set = normalized_source_data.get(field, set()) # 使用标准化的源值
                    resulting_set = current_target_set

                    if copy_mode == "overwrite":
                        if source_set != current_target_set: resulting_set = source_set; field_changed = True
                    elif copy_mode == "append":
                        resulting_set = current_target_set.union(source_set)
                        if resulting_set != current_target_set: field_changed = True
                    elif copy_mode == "remove":
                        resulting_set = current_target_set.difference(source_set)
                        if resulting_set != current_target_set: field_changed = True
                    else:
                        logging.debug(f"目标 {target_name_for_log}: 列表字段 '{field}' 不支持模式 '{copy_mode}'，跳过。")
                        continue
                    if field_changed:
                        try:
                            new_value = target_tool.format_list_field_for_api(field, resulting_set)
                            logging.debug(f"目标 {target_name_for_log}: 字段 '{field}' 格式化列表结果: {repr(new_value)}")
                        except AttributeError:
                            logging.error(f"目标工具 {type(target_tool).__name__} 缺少 format_list_field_for_api 方法！")
                            field_processing_errors = True; continue # 跳过此字段
                        except Exception as fmt_e:
                            logging.error(f"格式化列表字段 '{field}' 时出错: {fmt_e}", exc_info=True)
                            field_processing_errors = True; continue # 跳过此字段

                # --- 字典字段处理 ---
                elif field in DICT_FIELDS:
                    current_target_dict = normalize_to_dict(original_target_value, field, target_name_for_log)
                    source_dict = normalized_source_data.get(field, {}) # 使用标准化的源值
                    resulting_dict = current_target_dict.copy()

                    if copy_mode == "overwrite":
                        if source_dict != current_target_dict: resulting_dict = source_dict; field_changed = True
                    elif copy_mode == "merge":
                        temp_dict = current_target_dict.copy()
                        temp_dict.update(source_dict) # 合并，源覆盖目标
                        if temp_dict != current_target_dict: resulting_dict = temp_dict; field_changed = True
                    elif copy_mode == "delete_keys":
                        keys_to_delete = []
                        # delete_keys 使用的是原始 source_value (需要是列表或字符串)
                        if isinstance(source_value, list): keys_to_delete = source_value
                        elif isinstance(source_value, str): keys_to_delete = [k.strip() for k in source_value.split(',') if k.strip()]
                        else:
                            logging.warning(f"目标 {target_name_for_log}: 字典字段 '{field}' 的 'delete_keys' 模式需源值为列表/字符串，收到 {type(source_value)}，跳过。")
                            continue
                        original_len = len(resulting_dict)
                        deleted_keys_count = 0
                        for key in keys_to_delete:
                            if key in resulting_dict:
                                del resulting_dict[key]
                                deleted_keys_count += 1
                        if deleted_keys_count > 0: field_changed = True # 只要删除了key就算改变
                    else:
                        logging.debug(f"目标 {target_name_for_log}: 字典字段 '{field}' 不支持模式 '{copy_mode}'，跳过。")
                        continue
                    if field_changed:
                        try:
                            new_value = target_tool.format_dict_field_for_api(field, resulting_dict)
                            logging.debug(f"目标 {target_name_for_log}: 字段 '{field}' 格式化字典结果: {repr(new_value)}")
                        except AttributeError:
                            logging.error(f"目标工具 {type(target_tool).__name__} 缺少 format_dict_field_for_api 方法！")
                            field_processing_errors = True; continue # 跳过此字段
                        except Exception as fmt_e:
                            logging.error(f"格式化字典字段 '{field}' 时出错: {fmt_e}", exc_info=True)
                            field_processing_errors = True; continue # 跳过此字段

                # --- 简单字段处理 (仅支持 overwrite) ---
                else:
                    if copy_mode == "overwrite":
                        # 直接比较原始值
                        if source_value != original_target_value:
                            new_value = source_value
                            field_changed = True
                    else:
                        logging.debug(f"目标 {target_name_for_log}: 简单字段 '{field}' 仅支持 'overwrite' 模式，模式 '{copy_mode}' 无效，跳过。")
                        continue

                # --- 记录变更 ---
                if field_changed:
                    try:
                        # 调用最终格式化方法，确保类型等符合 API 要求
                        formatted_new_value = target_tool.format_field_value_for_api(field, new_value)
                        logging.debug(f"目标 {target_name_for_log}: 字段 '{field}' 最终格式化值: {repr(formatted_new_value)}")
                    except AttributeError:
                        logging.error(f"目标工具 {type(target_tool).__name__} 缺少 format_field_value_for_api 方法！")
                        field_processing_errors = True; continue # 跳过此字段
                    except Exception as format_e:
                        logging.error(f"最终格式化字段 '{field}' 值时出错: {format_e}", exc_info=True)
                        field_processing_errors = True; continue # 跳过此字段

                    # 将最终格式化后的值放入 payload
                    payload_for_api[field] = formatted_new_value
                    # 变更摘要仍然使用处理过程中的 new_value，因为它更易读
                    change_str = f"'{repr(original_target_value)}' -> '{repr(new_value)}' ({copy_mode})"
                    changes_summary[field] = change_str
                    logging.debug(f"目标 {target_name_for_log}: 字段 '{field}' 将修改为: {change_str} (API值: {repr(formatted_new_value)})")

            except AttributeError as e:
                # 捕获 _normalize 或 format 方法缺失
                logging.error(f"处理目标 {target_name_for_log} 字段 '{field}' 时缺少方法: {e}", exc_info=True)
                print(f"警告：处理目标 '{target_name}' (ID: {target_id}) 字段 '{field}' 时代码内部错误 (缺少方法)，已跳过。查日志。")
                field_processing_errors = True
                continue # 跳到下一个字段
            except Exception as e:
                logging.error(f"处理目标 {target_name_for_log} 的字段 '{field}' 时出错: {e}", exc_info=True)
                print(f"警告：处理目标 '{target_name}' (ID: {target_id}) 字段 '{field}' 时出错，已跳过。查日志。")
                field_processing_errors = True
                continue # 跳到下一个字段

        # 如果计算出此目标渠道有变更，则加入计划
        if changes_summary:
            logging.info(f"为目标 ID: {target_id}, Name: '{target_name}' 计算出 {len(changes_summary)} 项变更。")
            update_plan.append({
                "target_id": target_id,
                "target_name": target_name,
                "payload": payload_for_api, # 只包含 ID 和变更字段的字典
                "changes_summary": changes_summary, # 变更描述字典
                "original_data": copy.deepcopy(target_channel) # 保存原始数据以供撤销
            })
        elif not field_processing_errors: # 没有变更且没有字段处理错误
            logging.info(f"目标 ID: {target_id}, Name: '{target_name}' 无需更新。")
        # 如果有字段处理错误但无变更，则不加入 plan，日志中应有记录

    # 2. 模拟运行与确认
    if not update_plan:
        print("\n经过计算，没有检测到任何需要应用的变更。")
        logging.info("没有需要应用的变更。")
        return 0

    print("\n--- 计划变更 (模拟运行) ---")
    print(f"将从源渠道 ID: {source_channel_data.get('id')}, Name: '{source_channel_data.get('name')}' 复制字段")
    print(f"共计划更新 {len(update_plan)} 个目标渠道:")
    for item in update_plan:
        print(f"\n -> 目标渠道: ID={item['target_id']}, Name='{item['target_name']}'")
        for field, change_desc in item['changes_summary'].items():
            print(f"    - {field}: {change_desc}")
    print("--------------------------")

    confirmed = False
    if args.yes:
        confirmed = True
        logging.info("自动确认模式 (-y)：将执行以上所有变更。")
    else:
        try:
            confirm_input = input(f"确认要将以上变更应用到 {len(update_plan)} 个目标渠道吗？ (y/n): ").lower()
            if confirm_input == 'y':
                confirmed = True
            else:
                print("操作已取消。")
                return 0
        except EOFError: # 处理管道或重定向输入结束的情况
            print("\n输入结束，操作已取消。")
            return 0

    # 3. 执行更新 (如果确认)
    if confirmed:
        success_count = 0
        failure_count = 0
        failed_updates = [] # 存储失败的 (id, name, reason)

        # --- 先保存撤销数据 ---
        original_targets_for_undo = [item['original_data'] for item in update_plan]
        if original_targets_for_undo:
            try:
                target_config_name = target_config_path.stem
                logging.info(f"尝试为目标站点 '{target_config_path.stem}' 保存 {len(original_targets_for_undo)} 条撤销数据...")
                # 调用更新后的 save_undo_data，直接传递预取的数据
                await save_undo_data(
                    api_config_path=target_config_path, # 传递完整的路径
                    channels_to_save=original_targets_for_undo
                )
                logging.info(f"为目标站点 '{target_config_path.stem}' 成功保存撤销数据。")
                print(f"\n已为目标站点 '{target_config_path.stem}' 保存 {len(original_targets_for_undo)} 条撤销信息。")
            except Exception as undo_e:
                logging.error(f"为目标站点 '{target_config_path.stem}' 保存撤销数据时出错: {undo_e}", exc_info=True)
                print(f"\n警告：未能保存撤销数据！如果继续执行更新，将无法撤销。")
                # 撤销失败时，再次向用户确认是否继续
                if not args.yes:
                    try:
                        continue_anyway = input("无法保存撤销数据。是否仍要继续执行更新操作？ (y/n): ").lower()
                        if continue_anyway != 'y':
                            print("操作已取消。")
                            return 0
                        else:
                            print("警告：将继续执行更新，但无法撤销。")
                            logging.warning("用户选择在撤销数据保存失败后继续执行更新。")
                    except EOFError:
                        print("\n输入结束，操作已取消。")
                        return 0
                else:
                    print("警告：自动确认模式下，即使撤销数据保存失败，也将继续执行更新。")
                    logging.warning("自动确认模式：撤销数据保存失败，但继续执行更新。")

        # --- 并发执行更新 ---
        api_settings = script_config.get('api_settings', {})
        concurrency_limit = api_settings.get('max_concurrent_requests', 5)
        request_interval_ms = api_settings.get('request_interval_ms', 0)
        interval_seconds = request_interval_ms / 1000.0 if request_interval_ms > 0 else 0
        
        semaphore = Semaphore(concurrency_limit)
        update_tasks = []

        # 内部 async 函数用于执行单个更新任务
        async def update_single_channel_task(item):
            target_id = item['target_id']
            target_name = item['target_name']
            payload = item['payload'] # 包含 ID 和变更字段
            async with semaphore: # 控制并发
                # 在发送请求前，如果配置了间隔，则等待
                if interval_seconds > 0:
                    logging.debug(f"等待 {interval_seconds:.3f} 秒后更新目标 ID: {target_id}...")
                    await asyncio.sleep(interval_seconds)

                logging.info(f"开始更新目标渠道 ID: {target_id}, Name: '{target_name}'...")
                logging.debug(f"发送到 API 的载荷 (ID: {target_id}): {json.dumps(payload, indent=2, ensure_ascii=False)}")
                try:
                    success, message = await target_tool.update_channel_api(payload)
                    if success:
                        logging.info(f"成功更新目标 ID: {target_id}, Name: '{target_name}': {message}")
                        return True, target_id, target_name, None # 返回成功状态和标识
                    else:
                        logging.error(f"更新目标 ID: {target_id}, Name: '{target_name}' 失败: {message}")
                        return False, target_id, target_name, message # 返回失败状态和原因
                except Exception as e:
                    logging.error(f"更新目标 ID: {target_id}, Name: '{target_name}' 时意外错误: {e}", exc_info=True)
                    err_msg = f"发生意外错误: {e}"
                    return False, target_id, target_name, err_msg # 返回失败状态和原因

        # 创建所有更新任务
        for item in update_plan:
            update_tasks.append(update_single_channel_task(item))

        # 执行任务并等待结果
        print(f"\n开始并发更新 {len(update_tasks)} 个目标渠道 (并发数: {concurrency_limit}, 请求间隔: {interval_seconds:.3f}s)...")
        results = await asyncio.gather(*update_tasks) # results 是 (success, id, name, reason) 的列表
        print("所有更新任务已完成。")

        # 处理结果
        for success, tid, tname, reason in results:
            if success:
                success_count += 1
            else:
                failure_count += 1
                failed_updates.append((tid, tname, reason))

        # 4. 报告结果
        print("\n--- 更新结果 ---")
        print(f"成功更新: {success_count} 个渠道")
        print(f"更新失败: {failure_count} 个渠道")
        if failed_updates:
            print("\n失败详情:")
            for fid, fname, freason in failed_updates:
                print(f"  - ID: {fid}, Name: '{fname}', 原因: {freason}")
        print("----------------")

        return 1 if failure_count > 0 else 0 # 如果有失败则返回 1

    else:
        return 0 # 用户未确认
async def execute_compare_fields(
    source_tool: 'ChannelToolBase',
    target_tool: 'ChannelToolBase',
    source_channel_data: Dict[str, Any],
    matched_target_channels: List[Dict[str, Any]],
    fields_to_compare: List[str]
) -> None:
    """
    执行比较字段的核心逻辑：标准化数据、逐个比较并打印差异。

    Args:
        source_tool: 源站点工具实例。
        target_tool: 目标站点工具实例。
        source_channel_data: 已确定的单个源渠道数据字典。
        matched_target_channels: 筛选出的目标渠道列表。
        fields_to_compare: 需要比较的字段名列表。
    """
    # --- 比较字段逻辑 (从 cross_site_handler 移入) ---
    print("\n--- 字段比较结果 ---")
    print(f"源渠道: ID={source_channel_data.get('id', 'N/A'):<4} Name='{source_channel_data.get('name', 'N/A')}'")
    print(f"将与以下 {len(matched_target_channels)} 个目标渠道进行比较:")
    overall_differences_found = False # 标记在所有比较中是否发现差异

    # 预先标准化源数据一次
    normalized_source_data = {}
    source_name_for_log = source_channel_data.get('name', f"ID:{source_channel_data.get('id')}")
    try:
        for field in fields_to_compare:
            source_value = source_channel_data.get(field)
            if field in LIST_FIELDS:
                normalized_source_data[field] = normalize_to_set(source_value)
            elif field in DICT_FIELDS:
                normalized_source_data[field] = normalize_to_dict(source_value, field, source_name_for_log)
            else:
                normalized_source_data[field] = source_value # 简单值直接用
    except Exception as norm_e:
        logging.error(f"标准化源渠道字段时出错: {norm_e}", exc_info=True)
        print(f"错误：标准化源渠道数据时出错，无法进行比较。")
        return # 无法标准化源，无法继续比较
    except Exception as norm_e:
        logging.error(f"标准化源渠道字段时出错: {norm_e}", exc_info=True)
        print(f"错误：标准化源渠道数据时出错，无法进行比较。")
        return # 无法标准化源，无法继续比较

    # 遍历每个目标渠道进行比较
    for target_channel in matched_target_channels:
        target_id = target_channel.get('id')
        target_name = target_channel.get('name', f"ID:{target_id}")
        print(f"\n -> 比较目标: ID={target_id}, Name='{target_name}'")
        target_differences_found = False # 标记此目标是否有差异

        for field in fields_to_compare:
            normalized_source_value = normalized_source_data.get(field) # 使用预先标准化的源值
            target_value = target_channel.get(field)
            target_name_for_log = target_name # 用于日志/错误

            try:
                # 标准化目标值并比较
                if field in LIST_FIELDS:
                    target_set = normalize_to_set(target_value)
                    if normalized_source_value == target_set:
                        pass # 相同则不打印
                    else:
                        target_differences_found = True
                        overall_differences_found = True
                        print(f"    - {field}: 不同")
                        print(f"      源: {','.join(sorted(list(normalized_source_value or set())))}") # 处理 None
                        print(f"      目标: {','.join(sorted(list(target_set or set())))}") # 处理 None
                elif field in DICT_FIELDS:
                    target_dict = normalize_to_dict(target_value, field, target_name_for_log)
                    if normalized_source_value == target_dict:
                        pass # 相同则不打印
                    else:
                        target_differences_found = True
                        overall_differences_found = True
                        print(f"    - {field}: 不同")
                        print(f"      源: {json.dumps(normalized_source_value, ensure_ascii=False, indent=2)}")
                        print(f"      目标: {json.dumps(target_dict, ensure_ascii=False, indent=2)}")
                else: # 简单字段
                    # 直接比较原始值 (源值已在 normalized_source_data 中)
                    if normalized_source_value == target_value:
                        pass # 相同则不打印
                    else:
                        target_differences_found = True
                        overall_differences_found = True
                        print(f"    - {field}: 不同")
                        print(f"      源: {repr(normalized_source_value)}")
                        print(f"      目标: {repr(target_value)}")
            except AttributeError as e:
                logging.error(f"比较目标 {target_name_for_log} 字段 '{field}' 时缺少标准化方法: {e}", exc_info=True)
                print(f"    - {field}: 比较时出错 (代码内部错误，缺少方法，查日志)")
                target_differences_found = True # 出错也算作差异
                overall_differences_found = True
                continue # 继续比较下一个字段
            except Exception as comp_e:
                logging.error(f"比较目标 {target_name_for_log} 的字段 '{field}' 时出错: {comp_e}", exc_info=True)
                print(f"    - {field}: 比较时出错 (详情见日志)")
                target_differences_found = True # 出错也算作差异
                overall_differences_found = True
                continue # 继续比较下一个字段

        if not target_differences_found:
            print("    (所有比较字段均与源相同)")

    print("\n--- 比较总结 ---")
    if not overall_differences_found:
        print("所有目标渠道的所有待比较字段均与源渠道相同。")
    else:
        print("在比较中发现字段差异。")
    print("----------------")
    # 比较操作总是返回 None (或隐式 None)，因为它不表示失败状态，只打印结果