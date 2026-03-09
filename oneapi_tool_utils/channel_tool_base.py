# -*- coding: utf-8 -*-
"""
共享的基础模块，包含通道更新工具的抽象基类和通用功能。
"""
import abc
import requests # requests is still needed for exceptions etc., even if session creation moved
import json # 仍然需要用于 API payload 的 dumps
import yaml # 导入 YAML 库
import logging
import aiohttp
import asyncio
import copy
from pathlib import Path
import os # 用于检查文件修改时间
import re # 导入正则表达式模块

from channel_manager_lib.config_utils import load_yaml_config as load_base_yaml_config # 避免与 config_loaders 中的混淆，重命名基础加载器
from .network_utils import create_retry_session # 从新模块导入
from .config_loaders import load_api_config, load_update_config # 从新模块导入
from .data_helpers import normalize_to_set, normalize_to_dict # 从新模块导入
from .filtering_utils import filter_channels # 从新模块导入

# --- 常量 ---
# 定义缓存目录相对于此文件的位置 (或者从 main 导入)
# 假设 main_tool.py 和 oneapi_tool_utils 在同一级
# 注意：如果脚本从不同位置运行，这个相对路径可能需要调整
# LOADED_CONNECTION_CONFIG_DIR 已移至 config_loaders.py
MAX_PAGES_TO_FETCH = 500 # 最大获取页数限制

# --- 通用工具函数 ---
# load_yaml_config 已移至 config_utils.py
# create_retry_session 已移至 network_utils.py

# --- 抽象基类 ---
class ChannelToolBase(abc.ABC):
    """渠道更新工具的抽象基类"""

    def __init__(self, api_config_path, update_config_path=None, script_config=None):
        """
        初始化工具实例。

        Args:
            api_config_path (str): API 连接配置文件的路径。
            update_config_path (str, optional): 更新规则配置文件的路径。
                                                如果为 None (例如在撤销操作中)，则不加载。
            script_config (dict, optional): 加载后的脚本通用配置字典。
        """
        # 调用导入的加载函数
        self.api_config = load_api_config(api_config_path)
        self.update_config = load_update_config(update_config_path) if update_config_path else None
        # 存储传入的脚本配置，如果未提供则加载默认值（尽管调用者应确保提供）
        # 注意: load_script_config() 函数需要在 channel_manager_lib.config_utils 中定义
        # 如果 script_config 为 None，尝试加载默认配置
        if script_config is None:
            try:
                from channel_manager_lib.config_utils import load_script_config
                self.script_config = load_script_config()
            except ImportError:
                logging.warning("无法导入 load_script_config，将使用空字典作为 script_config。")
                self.script_config = {}
        else:
             self.script_config = script_config

        self.session = create_retry_session() # 同步 session 用于 get_all_channels
        # aiohttp session 在需要时创建

    # _load_api_config 和 _load_update_config 已移至 config_loaders.py

    @property
    def site_url(self):
        return self.api_config.get('site_url', '')

    @property
    def api_token(self):
        return self.api_config.get('api_token', '')

    @property
    def user_id(self):
        # user_id 是可选的
        return self.api_config.get('user_id', '1') # 提供默认值 '1'

    def get_api_type(self) -> str | None:
        """返回此工具实例的 API 类型 ('newapi' 或 'voapi')。"""
        return self.api_config.get('api_type')

    @abc.abstractmethod
    def get_all_channels(self):
        """
        获取所有渠道的列表。
        子类必须实现此方法以适应特定的 API。
        应返回包含两个元素的元组: (渠道字典列表 | None, 消息字符串)。
        如果成功，列表包含数据，消息为成功信息。
        如果失败，列表为 None，消息为错误描述。
        """
        pass

    @abc.abstractmethod
    async def update_channel_api(self, channel_data_payload):
        """
        调用 API 更新单个渠道。
        子类必须实现此方法以适应特定的 API 端点和认证。
        Args:
            channel_data_payload (dict): 包含渠道 ID 和待更新字段的字典。
        Returns:
            tuple[bool, str]: (更新是否成功, 包含成功或失败信息的消息字符串)
        """
        pass

    @abc.abstractmethod
    async def get_channel_details(self, channel_id):
        """
        获取单个渠道的详细信息。
        子类必须实现此方法以适应特定的 API 端点。
        Args:
            channel_id (int): 渠道 ID。
        Returns:
            tuple[dict | None, str]: (渠道的详细数据字典 | None, 包含成功或失败信息的消息字符串)
        """
        pass # Needed for save_undo_data

    @abc.abstractmethod
    def format_list_field_for_api(self, field_name: str, data_set: set) -> str | list:
        """
        将处理后的集合数据格式化为目标 API 更新时所需的格式。
        例如，转换为逗号分隔的字符串或列表本身。
        子类必须实现此方法。

        Args:
            field_name (str): 字段名称 (例如 "models", "group")。
            data_set (set): 包含最终值的集合。

        Returns:
            str | list: 格式化后的值，用于 API payload。
        """
        pass

    @abc.abstractmethod
    def format_dict_field_for_api(self, field_name: str, data_dict: dict) -> str | dict:
        """
        将处理后的字典数据格式化为目标 API 更新时所需的格式。
        例如，返回字典本身或 JSON 字符串。
        子类必须实现此方法。

        Args:
            field_name (str): 字段名称 (例如 "model_mapping", "setting")。
            data_dict (dict): 包含最终键值的字典。

        Returns:
            str | dict: 格式化后的值，用于 API payload。
        """
        pass

    @abc.abstractmethod
    def format_field_value_for_api(self, field_name: str, value: any) -> any:
        """
        对计算出的最终字段值进行特定于 API 的最后格式化。
        这主要用于确保简单字段类型正确（例如，整数、布尔值、字符串）。
        对于列表和字典字段，此方法可以在 format_list/dict_field_for_api 之后调用，
        或者那两个方法可以直接返回最终格式。子类应决定最佳实现。

        Args:
            field_name (str): 字段名称。
            value (any): 由 copy_fields 逻辑计算出的值。

        Returns:
            any: 最终格式化后，用于放入 API payload 的值。
        """
        pass

    @abc.abstractmethod
    async def test_channel_api(self, channel_id: int, model_name: str) -> tuple[bool, str, str | None]:
        """
        使用指定的模型测试单个渠道。

        Args:
            channel_id (int): 要测试的渠道的 ID。
            model_name (str): 用于测试的模型名称。

        Returns:
            tuple[bool, str, str | None]: (测试是否通过, 描述信息, 失败类型)
                                          失败类型: 'quota', 'auth', 'api_error', 'server_error',
                                                    'response_format', 'timeout', 'network',
                                                    'config', 'exception', None (成功时)
        """
        pass

    def filter_channels(self, channel_list: list, filters_config: dict | None = None) -> list:
        """
        根据提供的筛选器配置过滤渠道列表。
        这是一个便利方法，用于调用 filtering_utils 中的 filter_channels 函数。
        """
        # 直接调用导入的 filter_channels 函数
        return filter_channels(channel_list, filters_config)

    # 移除的方法: _validate_match_mode, _match_filter, _channel_matches_filters,
    #           _normalize_to_set, _normalize_to_dict

    def _prepare_update_payload(self, original_channel_data):
        """
        根据更新配置为单个渠道准备 API 更新所需的 payload。

        Args:
            original_channel_data (dict): 原始渠道数据。

        Returns:
            tuple: (dict | None, set)
                - dict | None: 准备好的更新 payload (包含 id 和需要更新的字段)。
                               如果无需更新或出错，则为 None。
                - set: 实际发生变更的字段名称集合。
        """
        # 检查 update_config 是否已加载
        if self.update_config is None or 'updates' not in self.update_config:
            logging.error("准备更新 payload 失败：更新配置文件未加载或缺少 'updates' 部分。")
            return None, set()

        updates_config = self.update_config.get('updates', {})
        channel_id = original_channel_data.get('id')
        if not channel_id:
            logging.error("准备更新 payload 失败：原始渠道数据缺少 ID。")
            return None, set()

        payload = {'id': channel_id} # 始终包含 ID
        changed_fields = set()
        channel_name = original_channel_data.get('name', f'ID:{channel_id}') # 用于日志

        for field, config in updates_config.items():
            if not isinstance(config, dict) or config.get("enabled") is not True:
                continue # 跳过未启用或格式错误的更新

            # --- 获取原始值 ---
            original_value = original_channel_data.get(field)
            
            # --- 智能获取模式列表 ---
            # 支持同时配置多个模式，例如同时 append 和 regex_replace
            possible_modes = ["overwrite", "append", "remove", "merge", "delete_keys", "regex_replace"]
            
            # 确定要执行的任务序列
            tasks = [] # 存储 (mode, value) 元组
            
            if config.get("mode"):
                # 标准格式: {mode: "append", value: [...]}
                tasks.append((config.get("mode"), config.get("value")))
            else:
                # 快捷格式: {append: [...], regex_replace: [...]}
                for m in possible_modes:
                    if m in config:
                        val = config.get(m)
                        tasks.append((m, val))
            
            # 如果没有任何模式，尝试默认 overwrite
            if not tasks:
                if 'value' in config:
                    tasks.append(("overwrite", config.get('value')))
                else:
                    continue

            # --- 顺序执行所有任务 ---
            current_value = original_value
            field_changed_in_loop = False
            
            for mode, update_value in tasks:
                # 检查 update_value 是否获取成功 (对于需要值的模式)
                if update_value is None and mode not in ["delete_keys"]:
                    logging.warning(f"渠道 {channel_name} 的字段 '{field}' (模式: {mode}) 缺少值。跳过此子任务。")
                    continue

                new_value = None
                try:
                    # 模式 1: overwrite
                    if mode == "overwrite":
                        if isinstance(update_value, list):
                            new_value = self.format_list_field_for_api(field, set(update_value))
                        elif isinstance(update_value, dict):
                            new_value = self.format_dict_field_for_api(field, update_value)
                        else:
                            new_value = update_value

                    # 模式 2: regex_replace
                    elif mode == "regex_replace":
                        if field == "models" and isinstance(update_value, list):
                            # 规范化当前列表
                            curr_list = []
                            if isinstance(current_value, str):
                                curr_list = [m.strip() for m in current_value.split(',') if m.strip()]
                            elif isinstance(current_value, list):
                                curr_list = [str(m).strip() for m in current_value if str(m).strip()]
                            
                            processed_list = curr_list
                            for rule in update_value:
                                if isinstance(rule, dict) and 'pattern' in rule and 'replacement' in rule:
                                    try:
                                        processed_list = [re.sub(rule['pattern'], rule['replacement'], model) for model in processed_list]
                                    except re.error as re_err:
                                        logging.warning(f"渠道 {channel_name} 的字段 '{field}' 正则替换规则无效: {rule} ({re_err})。")
                                        continue
                            final_list = list(dict.fromkeys(processed_list))
                            new_value = self.format_list_field_for_api(field, final_list)
                        elif isinstance(current_value, str) and isinstance(update_value, dict) and \
                           'pattern' in update_value and 'replacement' in update_value:
                            try:
                                new_value = re.sub(update_value['pattern'], update_value['replacement'], current_value)
                            except re.error as re_err:
                                logging.warning(f"渠道 {channel_name} 的字段 '{field}' 正则替换失败: {re_err}")
                                continue
                        else:
                            continue

                    # 模式 3: append
                    elif mode == "append":
                        curr_set = normalize_to_set(current_value)
                        upd_set = normalize_to_set(update_value)
                        new_value = self.format_list_field_for_api(field, curr_set.union(upd_set))

                    # 模式 4: remove
                    elif mode == "remove":
                        if field == "models":
                            curr_list = []
                            if isinstance(current_value, str):
                                curr_list = [m.strip() for m in current_value.split(',') if m.strip()]
                            elif isinstance(current_value, list):
                                curr_list = [str(m).strip() for m in current_value if str(m).strip()]
                            
                            to_rem = []
                            if isinstance(update_value, str):
                                to_rem = [m.strip() for m in update_value.split(',') if m.strip()]
                            elif isinstance(update_value, list):
                                to_rem = [str(m).strip() for m in update_value if str(m).strip()]
                            
                            new_value = self.format_list_field_for_api(field, [m for m in curr_list if m not in to_rem])
                        else:
                            curr_set = normalize_to_set(current_value)
                            rem_set = normalize_to_set(update_value)
                            new_value = self.format_list_field_for_api(field, curr_set - rem_set)

                    # 模式 5: merge
                    elif mode == "merge":
                        curr_dict = normalize_to_dict(current_value, field, channel_name)
                        upd_dict = normalize_to_dict(update_value, field, channel_name)
                        final_dict = copy.deepcopy(curr_dict)
                        final_dict.update(upd_dict)
                        new_value = self.format_dict_field_for_api(field, final_dict)

                    # 模式 6: delete_keys
                    elif mode == "delete_keys":
                        curr_dict = normalize_to_dict(current_value, field, channel_name)
                        keys_to_del = normalize_to_set(update_value)
                        final_dict = copy.deepcopy(curr_dict)
                        for key in keys_to_del:
                            if key in final_dict:
                                del final_dict[key]
                        new_value = self.format_dict_field_for_api(field, final_dict)

                    # --- 检查子任务是否产生了变化 ---
                    formatted_new = self.format_field_value_for_api(field, new_value)
                    formatted_curr = self.format_field_value_for_api(field, current_value)
                    
                    is_changed = False
                    if formatted_new is None and formatted_curr: is_changed = True
                    elif formatted_curr is None and formatted_new: is_changed = True
                    elif isinstance(formatted_curr, list) and isinstance(formatted_new, list):
                        if sorted(formatted_curr) != sorted(formatted_new): is_changed = True
                    elif formatted_curr != formatted_new: is_changed = True

                    if is_changed:
                        current_value = formatted_new
                        field_changed_in_loop = True

                except Exception as e:
                    logging.error(f"处理渠道 {channel_name} 字段 '{field}' (模式: {mode}) 时出错: {e}")
                    continue
            
            if field_changed_in_loop:
                payload[field] = current_value
                changed_fields.add(field)

        if not changed_fields:
            logging.debug(f"渠道 {channel_name} (ID: {channel_id}) 没有需要更新的字段。")
            return None, set()

        return payload, changed_fields

    async def run_updates(self, dry_run=False):
        """
        (已废弃/或待重构) 统一执行更新流程 (获取、过滤、准备、执行)。
        注意：此方法目前未使用，推荐使用 single_site_handler 中的流程。
              如果需要保留，需要重构以适应新的并发和错误处理逻辑。

        Args:
            dry_run (bool): 如果为 True，则只打印计划的更新而不执行。

        Returns:
            int: 0 表示成功或无操作, 1 表示失败, 3 表示严重错误。
        """
        logging.warning("run_updates 方法已不推荐使用，请使用 single_site_handler 中的流程。")
        # --- 1. 获取和过滤渠道 ---
        # get_all_channels 现在是同步方法，需要调整为异步或在同步上下文中处理
        # 假设在异步上下文中调用
        try:
            # 需要调用异步版本的 get_all_channels
            # 或者假设 get_all_channels 已经在外部调用并传入
            # 这里无法直接调用，因为 run_updates 设计为同步方法
            # all_channels, get_list_message = await self.get_all_channels_async() # 假设有异步版本
            logging.error("run_updates 无法直接调用异步的 get_all_channels，需要重构。")
            return 3 # 结构性问题

            # if all_channels is None:
            #     logging.error(f"获取渠道列表失败: {get_list_message}")
            #     return 1
            # if not all_channels:
            #     logging.info(f"渠道列表为空 ({get_list_message})，无需更新。")
            #     return 0
        except AttributeError:
             logging.error("假设的 get_all_channels_async 方法不存在。run_updates 需要重构。")
             return 3

        # 使用 self.update_config['filters'] 进行过滤
        filters_config = self.update_config.get('filters') if self.update_config else None
        # 需要使用导入的 filter_channels 函数
        # all_channels 变量在此处未定义，需要从外部传入或者在方法开始处获取
        # filtered_channels = filter_channels(all_channels, filters_config)


        # if not filtered_channels:
        #     logging.info("没有匹配筛选条件的渠道。")
        #     return 0

        # logging.info(f"找到 {len(filtered_channels)} 个匹配的渠道，准备处理...")

        # --- 2. 准备更新任务 ---
        payloads_to_update = []
        channels_info = [] # 用于记录失败信息
        # filtered_channels 变量在此处未定义
        # for channel in filtered_channels:
            # channel_id = channel.get('id')
            # channel_name = channel.get('name', f'ID:{channel_id}')
            # payload, changed = self._prepare_update_payload(channel)
            # if payload and changed:
            #     payloads_to_update.append(payload)
            #     channels_info.append({'id': channel_id, 'name': channel_name})
            #     # 打印计划变更 (如果 dry_run 或需要模拟输出)
            #     if dry_run:
            #         print(f"\n计划更新渠道 {channel_name} (ID: {channel_id}):")
            #         for field in changed:
            #              original_value = channel.get(field)
            #              new_value = payload.get(field)
            #              print(f"  - {field}: {repr(original_value)} -> {repr(new_value)}")

        if not payloads_to_update:
            logging.info("所有匹配的渠道都无需更新。")
            if dry_run: print("\n模拟运行：没有检测到需要执行的更新。")
            return 0

        if dry_run:
            print("\n模拟运行结束。")
            return 0

        # --- 3. 执行更新 (需要改成异步并发) ---
        logging.info(f"开始执行 {len(payloads_to_update)} 个渠道的更新...")

        # 获取并发设置
        api_settings = self.script_config.get('api_settings', {})
        max_concurrent = api_settings.get('max_concurrent_requests', 5)
        semaphore = asyncio.Semaphore(max_concurrent)

        async def update_task_wrapper(payload):
            async with semaphore:
                # update_channel_api 是异步方法
                # 返回 tuple[bool, str]
                success, msg = await self.update_channel_api(payload)
                return success, msg # 返回元组

        tasks = [update_task_wrapper(p) for p in payloads_to_update]
        results = []
        try:
            # 需要在异步上下文中运行 asyncio.gather
            # 这个方法本身是同步的，不能直接 await gather
            # 再次强调：此方法需要重构为异步或由异步函数调用
            logging.error("run_updates 方法需要重构以支持异步更新执行。")
            return 1 # 表示失败，因为无法执行

            # 假设能拿到 results
            # results = await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
             logging.critical(f"执行更新流程时发生严重错误: {e}", exc_info=True)
             return 3 # 未知严重错误

        # --- 4. 处理结果 ---
        success_count = 0
        failure_count = 0
        # results 变量在此处未定义
        # for i, r in enumerate(results):
        #     if isinstance(r, tuple) and len(r) == 2:
        #         success, msg = r
        #         if success:
        #             success_count += 1
        #         else:
        #             failure_count += 1
        #             # 记录失败信息
        #             if i < len(channels_info):
        #                 ch_info = channels_info[i]
        #                 logging.error(f"渠道 {ch_info['name']} (ID: {ch_info['id']}) 更新失败: {msg}")
        #             else:
        #                  logging.error(f"未知渠道更新失败: {msg}")
        #     elif isinstance(r, Exception):
        #          failure_count += 1
        #          # 记录异常信息
        #          if i < len(channels_info):
        #              ch_info = channels_info[i]
        #              logging.error(f"渠道 {ch_info['name']} (ID: {ch_info['id']}) 更新时发生异常: {r}", exc_info=r)
        #          else:
        #              logging.error(f"未知渠道更新时发生异常: {r}", exc_info=r)
        #     else:
        #          failure_count += 1
        #          logging.error(f"更新任务返回未知结果类型: {r}")


        logging.info(f"更新任务完成: {success_count} 个成功, {failure_count} 个失败。")

        if failure_count > 0:
            return 1 # 返回 1 表示部分或全部失败
        else:
            return 0 # 返回 0 表示全部成功