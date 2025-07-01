# -*- coding: utf-8 -*-
"""
处理命令行参数解析和主要的交互式菜单逻辑。
"""
import argparse
import asyncio
import logging
from pathlib import Path
import yaml
import json # 添加导入
from typing import Literal # For _select_connection_config return type

# 从项目模块导入 (使用包内绝对导入)
from channel_manager_lib.config_utils import (
    CONNECTION_CONFIG_DIR, UPDATE_CONFIG_PATH, QUERY_CONFIG_PATH, CROSS_SITE_ACTION_CONFIG_PATH, # 导入 QUERY_CONFIG_PATH
    CLEAN_UPDATE_CONFIG_TEMPLATE_PATH, LOGS_DIR, DEFAULT_LOG_FILE_BASENAME,
    list_connection_configs, load_yaml_config, load_script_config, # 导入 YAML 加载函数和脚本配置加载函数
    # CHANNEL_MODEL_TEST_CONFIG_PATH # 将在 config_utils.py 中定义后取消注释
)
from channel_manager_lib.undo_utils import (
    perform_undo, find_latest_undo_file, find_latest_undo_file_for,
    get_undo_summary, _get_tool_instance # 导入撤销相关函数和工具实例获取
)
from channel_manager_lib.single_site_handler import run_single_site_operation, run_test_and_enable_disabled, run_test_model_on_channels # 导入单站点处理函数 (新增 run_test_model_on_channels)
from channel_manager_lib.cross_site_handler import run_cross_site_operation # 导入跨站点处理函数

# 默认的测试模型配置文件名，之后可以移到 config_utils.py
DEFAULT_CHANNEL_MODEL_TEST_CONFIG_NAME = "channel_model_test_config.yaml"
DEFAULT_CHANNEL_MODEL_TEST_CONFIG_PATH = Path(DEFAULT_CHANNEL_MODEL_TEST_CONFIG_NAME)


# --- Helper Functions ---

# ask_and_clear_update_config 已移至 single_site_handler.py

def select_config(configs: list[Path], auto_confirm=False) -> Path | None:
    """
    让用户通过数字选择配置文件，或在自动确认模式下跳过确认。
    会显示选定配置文件的内容。

    Args:
        configs (list[Path]): 可供选择的配置文件路径列表。
        auto_confirm (bool): 是否跳过用户确认。

    Returns:
        Path | None: 用户选择的配置文件路径，或在取消/错误时返回 None。
    """
    if not configs:
        # 注意：这里需要 CONNECTION_CONFIG_DIR 来提供更清晰的错误消息
        logging.error("错误：未找到可用的连接配置文件。")
        print(f"错误：未找到可用的连接配置文件。请确保 '{CONNECTION_CONFIG_DIR}' 目录下有 YAML 文件。")
        return None

    print("\n请选择要使用的连接配置:")
    for i, config_path in enumerate(configs):
        print(f"  {i + 1}: {config_path.name}")

    while True:
        try:
            choice = input(f"请输入选项编号 (1-{len(configs)}): ")
            index = int(choice) - 1
            if 0 <= index < len(configs):
                selected_path = configs[index]
                print(f"\n您选择了: {selected_path.name}")
                try:
                    # --- 显示配置内容：直接读取并解析选定的 YAML 文件 ---
                    with open(selected_path, 'r', encoding='utf-8') as f:
                        content = yaml.safe_load(f) # 需要导入 yaml
                    print("--- 配置内容 ---")
                    # 使用 yaml.dump 输出 YAML，设置缩进和允许 Unicode
                    print(yaml.dump(content, indent=2, allow_unicode=True, default_flow_style=False, sort_keys=False))
                    print("-----------------")
                    # --- 显示结束 ---

                    # 移除二次确认，选择后直接返回
                    logging.info(f"用户已选择配置: {selected_path.name}")
                    return selected_path
                except (FileNotFoundError, yaml.YAMLError, Exception) as e: # 捕获文件未找到或 YAML 解析错误
                     logging.error(f"读取或解析 YAML 配置文件 '{selected_path}' 时出错: {e}")
                     print(f"无法读取或解析此 YAML 配置文件 ({e})，请检查文件内容或日志，然后重新选择。")
                     # 重新显示选项
                     print("\n请选择要使用的连接配置:")
                     for i, config_path in enumerate(configs):
                         print(f"  {i + 1}: {config_path.name}")
            else:
                print(f"无效选项，请输入 1 到 {len(configs)} 之间的数字。")
        except ValueError:
            print("无效输入，请输入数字。")
        except EOFError:
            print("\n操作已取消。")
            return None # 返回 None 表示取消

def _select_connection_config(prompt_message: str, exclude_config: str | None = None) -> str | None | Literal["cancel"]:
    """
    列出 connection_configs 下的 YAML 文件并让用户选择一个。

    Args:
        prompt_message (str): 显示给用户的提示信息。
        exclude_config (str | None): 需要排除的配置文件名 (例如，跨站点操作时的源配置)。

    Returns:
        str | None | Literal["cancel"]:
            - 用户选择的配置文件的路径字符串。
            - 如果用户选择返回或没有可用配置，则返回 None。
            - 如果用户取消操作 (EOFError)，则返回 "cancel"。
    """
    # 使用已在顶部导入的 CONNECTION_CONFIG_DIR
    config_dir = CONNECTION_CONFIG_DIR
    try:
        available_configs = sorted([f for f in config_dir.glob('*.yaml') if f.is_file()])
    except FileNotFoundError:
        print(f"错误：连接配置目录 '{config_dir}' 不存在。")
        logging.error(f"连接配置目录 '{config_dir}' 不存在。")
        return None

    if exclude_config:
        try:
            exclude_path_name = Path(exclude_config).name
            available_configs = [f for f in available_configs if f.name != exclude_path_name]
        except Exception as e:
            logging.warning(f"处理排除配置 '{exclude_config}' 时出错: {e}")


    if not available_configs:
        print(f"错误：在 '{config_dir}' 目录下未找到可用的 YAML 配置文件。")
        logging.warning(f"在 '{config_dir}' 目录下未找到可用的 YAML 配置文件 (可能已被排除)。")
        return None

    print(f"\n{prompt_message}")
    for i, config_file in enumerate(available_configs):
        print(f"[{i+1}] {config_file.name}")
    print("[0] 返回上一级")

    while True:
        try:
            choice = input("请选择: ")
            if choice == '0':
                return None # 用户选择返回
            choice_index = int(choice) - 1
            if 0 <= choice_index < len(available_configs):
                selected_path = available_configs[choice_index]
                logging.info(f"用户选择配置文件: {selected_path.name}")
                return str(selected_path) # 返回路径字符串
            else:
                print(f"无效选项，请输入 0 到 {len(available_configs)} 之间的数字。")
        except ValueError:
            print("无效输入，请输入数字。")
        except EOFError:
            print("\n操作已取消。")
            return "cancel" # 特殊值表示取消整个操作

# --- Argument Parser Setup ---
def setup_arg_parser():
    """设置并返回命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="One API 渠道批量更新工具",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # --- 操作模式参数组 ---
    mode_group = parser.add_argument_group('运行模式 (选择一种或不选进入交互模式)')
    mode_action = mode_group.add_mutually_exclusive_group() # 确保 --update, --undo, --test-and-enable-disabled, --test-channel-model 互斥
    mode_action.add_argument(
        "--update",
        action="store_true",
        help="明确执行更新操作 (需要指定 --connection-config)。"
    )
    mode_action.add_argument(
        "--undo",
        action="store_true",
        help="执行撤销操作 (需要指定 --connection-config)。"
    )
    mode_action.add_argument(
        "--test-and-enable-disabled",
        action="store_true",
        help="测试自动禁用的渠道并尝试启用它们 (需要指定 --connection-config)。"
    )
    mode_action.add_argument(
        "--find-key",
        metavar="<API_KEY_TO_FIND>",
        help="查找指定 API Key 所在的渠道并打印其信息 (需要指定 --connection-config)。"
    )
    mode_action.add_argument(
        "--test-channel-model",
        metavar="<test_config_path>",
        help="根据指定的测试配置文件测试渠道对特定模型的支持情况。\n(例如: channel_model_test_config.yaml)"
    )
    mode_action.add_argument(
        "--cross-site",
        action="store_true",
        help="执行跨站点操作 (根据 cross_site_action.yaml 配置)。"
    )
    mode_action.add_argument(
        "--query",
        action="store_true",
        help="根据 query_config.yaml 的设置查询渠道信息 (需要指定 --connection-config)。"
    )
    # --- 参数组定义调整 ---
    # 将 --connection-config 保持为通用参数，但其适用性由各模式自行决定
    # --clear-config 属于更新操作
    # 新增 --clear-test-model-config 属于测试模型操作

    # 通用单实例连接配置 (某些模式需要)
    connection_config_group = parser.add_argument_group('连接配置 (部分模式需要)')
    connection_config_group.add_argument(
        "--connection-config",
        metavar="<path>",
        help=f"指定目标连接配置文件路径 (用于 --update, --undo, --test-and-enable-disabled, --find-key)。\n(例如: {CONNECTION_CONFIG_DIR}/my_config.yaml)"
    )
    
    # 更新特定参数
    update_options_group = parser.add_argument_group('更新操作特定选项 (当使用 --update 时)')
    update_options_group.add_argument(
        "--clear-config", # 这个参数与 --update 关联
        action="store_true",
        help=f"在 --update 操作成功完成后，将 '{UPDATE_CONFIG_PATH.name}' 恢复为默认干净状态。"
    )

    # 测试模型特定参数
    test_model_options_group = parser.add_argument_group('测试模型操作特定选项 (当使用 --test-channel-model 时)')
    test_model_options_group.add_argument(
        "--clear-test-model-config", # 新参数
        action="store_true",
        help=f"在 --test-channel-model 操作成功完成后，将指定的测试配置文件恢复为默认干净状态。"
    )

    # --- 通用控制参数 ---
    # shared_group 名字可能不再准确，改为 control_group 或 general_options
    # 此处的 shared_group = parser.add_argument_group('单站点目标 (用于 --update, --undo, --test-and-enable-disabled, --find-key)')
    # 应该被上面的 connection_config_group 和各特定选项组取代或重构。
    # 为了最小化改动，暂时保留旧的 shared_group 声明，但其实际内容已被细分。
    # 实际上，--connection-config 已被移到 connection_config_group。
    # --clear-config 已被移到 update_options_group。
    # 我们需要确保旧的 shared_group 不再包含这些。

    # 旧的 shared_group 定义将被新的分组覆盖或逻辑上失效，这里将其注释掉以明确
    # shared_group = parser.add_argument_group('单站点目标 (用于 --update, --undo, --test-and-enable-disabled, --find-key)')
    # shared_group.add_argument(
    #     "--connection-config", ...
    # )
    # update_group = parser.add_argument_group('更新选项 (仅在 --update 模式下有效)')
    # update_group.add_argument(
    #     "--clear-config", ...
    # )
    # 以上旧的 group 定义实际上已被新的更细致的 group 取代。
    # argparse 会按顺序处理，所以新的 group 会生效。
    # 保留 control_group 用于 -y/--yes。
    # 以下对 shared_group 的引用应被删除，因为其参数已移至新的分组
    # shared_group.add_argument(
    #     "--connection-config",
    #     metavar="<path>",
    #     help=f"指定单站点操作的目标连接配置文件路径。\n(例如: {CONNECTION_CONFIG_DIR}/my_config.yaml)"
    # )
    # # --api-type 参数已被移除

    # --- 通用控制参数 ---
    control_group = parser.add_argument_group('通用控制')
    control_group.add_argument(
        "-y", "--yes",
        action="store_true",
        help="自动确认所有提示 (用于非交互式运行)。"
    )

    # --- 日志参数组 (保留，因为 main_tool.py 需要解析它们来设置日志) ---
    log_group = parser.add_argument_group('日志选项')
    log_group.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="设置日志记录级别 (默认为 INFO)。"
    )
    log_group.add_argument(
        "--log-file",
        metavar="<path>",
        default=None,
        help=f"指定日志文件的具体路径或目录。\n默认为 '{LOGS_DIR}/{DEFAULT_LOG_FILE_BASENAME}' (带轮转)。\n如果提供空字符串 '' 或 'none'，则不写入文件。"
    )
    return parser


# --- 主交互和分发逻辑 ---
async def _execute_query(tool_instance: 'ChannelToolBase') -> int:
    """提取出的查询逻辑，供多处调用。"""
    channel_list, msg = await tool_instance.get_all_channels()
    if channel_list is None:
        logging.error(f"获取渠道列表失败: {msg}")
        print(f"错误：获取渠道列表失败。详情请查看日志。")
        return 1
    
    query_config = load_yaml_config(QUERY_CONFIG_PATH)
    processed_channel_list = channel_list

    if query_config and isinstance(query_config.get('filters'), dict) and query_config['filters']:
        query_filters = query_config['filters']
        logging.info(f"从 {QUERY_CONFIG_PATH.name} 加载查询筛选条件: {query_filters}")
        print(f"提示：将使用 {QUERY_CONFIG_PATH.name} 中定义的筛选条件进行查询。")
        processed_channel_list = tool_instance.filter_channels(channel_list, query_filters)
        if not processed_channel_list:
            logging.warning(f"根据 {QUERY_CONFIG_PATH.name} 中的筛选条件，未匹配到任何渠道。")
            print(f"根据 {QUERY_CONFIG_PATH.name} 中的筛选条件，未匹配到任何渠道。")
    else:
        logging.info(f"{QUERY_CONFIG_PATH.name} 中未定义筛选条件或 filters 为空，将显示所有获取到的渠道。")

    default_query_fields = ['id', 'name']
    query_fields = []
    if query_config and isinstance(query_config.get('query_fields'), list):
        query_fields = query_config['query_fields']
        logging.info(f"从 {QUERY_CONFIG_PATH.name} 加载查询显示字段: {query_fields}")
    else:
        query_fields = default_query_fields
        logging.warning(f"{QUERY_CONFIG_PATH.name} 未找到或 'query_fields' 无效/缺失，将使用默认显示字段: {query_fields}")
        if not (query_config and isinstance(query_config.get('filters'), dict) and query_config['filters']):
            print(f"提示：未找到或显示字段配置无效 ({QUERY_CONFIG_PATH.name})，将显示默认字段。")
    
    if not query_fields:
        query_fields = default_query_fields
        logging.warning("查询显示字段列表为空，强制使用默认字段。")

    print(f"\n查询到 {len(processed_channel_list)} 个渠道 (按 ID 排序)，显示字段: {', '.join(query_fields)}")
    processed_channel_list.sort(key=lambda c: c.get('id') if isinstance(c.get('id'), int) else float('inf'))

    header = " | ".join([f"{field}" for field in query_fields])
    separator = "-" * (len(header) + (len(query_fields) - 1) * 3)
    print(header)
    print(separator)

    for channel in processed_channel_list:
        row_data = []
        for field in query_fields:
            value = channel.get(field, 'N/A')
            if isinstance(value, (list, dict)):
                value_str = json.dumps(value, ensure_ascii=False)
            else:
                value_str = str(value) if value is not None else 'N/A'
            row_data.append(f"{value_str}")
        print(" | ".join(row_data))
    
    return 0

async def main_cli_entry(args):
    """
    CLI 处理主入口点：处理交互模式、分发到单站点或跨站点处理器。
    """
    final_exit_code = 0
    operation_mode = None # 'single_site' or 'cross_site'

    # 加载脚本通用配置 (只加载一次)
    script_config = load_script_config()
    logging.debug(f"CLI Handler 加载的脚本配置: {script_config}")


    # --- 1. 确定操作模式 ---
    is_interactive_mode = not args.update and \
                          not args.undo and \
                          not args.test_and_enable_disabled and \
                          not args.find_key and \
                          not args.test_channel_model and \
                          not args.cross_site and \
                          not args.query # 新增条件

    if args.cross_site:
        operation_mode = 'cross_site'
        logging.info("通过命令行参数 (--cross-site) 选择跨站操作模式。")
    elif args.query:
        operation_mode = 'query'
        action_flag = "--query"
        logging.info(f"通过命令行参数 ({action_flag}) 选择查询模式。")
        if not args.connection_config:
            print(f"错误：使用 {action_flag} 时必须通过 --connection-config 指定目标配置文件。")
            logging.error(f"命令行模式 ({action_flag}) 下未指定 --connection-config。")
            return 1
        connection_config_path = Path(args.connection_config)
        if not connection_config_path.is_file():
            print(f"错误：指定的连接配置文件不存在: {args.connection_config}")
            logging.error(f"指定的连接配置文件不存在: {args.connection_config}")
            return 1
    elif args.find_key:
        operation_mode = 'find_key'
        action_flag = "--find-key"
        logging.info(f"通过命令行参数 ({action_flag}) 选择查找 API Key 模式。")
        if not args.connection_config:
            print(f"错误：使用 {action_flag} 时必须通过 --connection-config 指定目标配置文件。")
            logging.error(f"命令行模式 ({action_flag}) 下未指定 --connection-config。")
            return 1
        connection_config_path = Path(args.connection_config)
        if not connection_config_path.is_file():
            print(f"错误：指定的连接配置文件不存在: {args.connection_config}")
            logging.error(f"指定的连接配置文件不存在: {args.connection_config}")
            return 1
    elif args.test_channel_model: # 新增的处理分支
        operation_mode = 'test_channel_model'
        action_flag = "--test-channel-model"
        logging.info(f"通过命令行参数 ({action_flag}) 选择测试指定模型渠道模式。")
        test_config_path_str = args.test_channel_model # 用户提供的路径
        
        # 检查配置文件是否存在
        test_config_path = Path(test_config_path_str)
        if not test_config_path.is_file():
            print(f"错误：指定的测试配置文件不存在: {test_config_path_str}")
            logging.error(f"指定的测试配置文件不存在: {test_config_path_str}")
            return 1
        
        # 后续会在这里调用处理函数
        # final_exit_code = await run_test_model_on_channels(args, script_config, str(test_config_path))
        # return final_exit_code # 暂时先直接返回，等待实现处理函数
        
    elif not is_interactive_mode: # Other non-interactive modes (update, undo, test_and_enable_disabled)
        operation_mode = 'single_site' # Default to single_site for update/undo/test_and_enable
        action_flag = ""
        if args.update: action_flag = "--update"
        elif args.undo: action_flag = "--undo"
        elif args.test_and_enable_disabled: action_flag = "--test-and-enable-disabled"
        # No need to check args.find_key or args.test_channel_model here, as they are handled above
        
        logging.info(f"通过命令行参数 ({action_flag}) 选择单站操作模式。")
        # 命令行模式下必须提供连接配置 (for update, undo, test_and_enable_disabled)
        if not args.connection_config:
            print(f"错误：使用 {action_flag} 时必须通过 --connection-config 指定目标配置文件。")
            logging.error(f"命令行模式 ({action_flag}) 下未指定 --connection-config。")
            return 1
        connection_config_path = Path(args.connection_config)
        if not connection_config_path.is_file():
             print(f"错误：指定的连接配置文件不存在: {args.connection_config}")
             logging.error(f"指定的连接配置文件不存在: {args.connection_config}")
             return 1
    else: # Interactive mode
        # 纯交互模式，询问用户
        print("\n请选择要执行的操作模式:")
        while True:
            try:
                # 交互模式暂不添加新选项，优先命令行实现
                choice = input("[1] 单站点批量更新/撤销/查询 [2] 跨站点渠道操作 [0] 退出: ")
                if choice == '1':
                    operation_mode = 'single_site'
                    logging.info("用户选择单站操作模式。")
                    break
                elif choice == '2':
                    operation_mode = 'cross_site'
                    logging.info("用户选择跨站操作模式。")
                    break
                elif choice == '0':
                    logging.info("用户选择退出。")
                    print("操作已取消。")
                    return 0 # 直接退出
                else:
                    print("无效选项，请输入 1, 2 或 0。")
            except EOFError:
                print("\n操作已取消。")
                return 0

    # --- 2. 根据模式执行相应逻辑 ---
    if operation_mode == 'single_site':
        logging.info("进入单站操作流程...")
        connection_config_path_str = None

        # 获取连接配置路径 (命令行或交互)
        if args.connection_config: # 命令行已提供并验证过
            connection_config_path_str = args.connection_config
        else: # 交互模式选择
            available_configs = list_connection_configs()
            if not available_configs:
                print(f"错误：在 '{CONNECTION_CONFIG_DIR}' 目录下未找到连接配置文件。")
                return 1
            selected_path_obj = select_config(available_configs, auto_confirm=args.yes) # select_config 在此模块
            if not selected_path_obj:
                logging.info("用户未选择连接配置，操作取消。")
                return 0
            connection_config_path_str = str(selected_path_obj)

        # API 类型固定为 newapi
        api_type = "newapi"
        logging.info(f"API 类型固定为: {api_type}")
        try:
            # 仍然加载配置以确保其有效
            api_config = load_yaml_config(connection_config_path_str)
            if not api_config:
                 print(f"错误：无法加载连接配置文件 '{Path(connection_config_path_str).name}'。")
                 return 1
        except Exception as e: # 捕获可能的加载错误
            logging.error(f"加载连接配置 '{connection_config_path_str}' 时出错: {e}", exc_info=True)
            print(f"错误：无法从 '{Path(connection_config_path_str).name}' 加载配置。请检查文件和日志。")
            return 1

        # --- 单站操作分发 (更新/撤销/查询) ---
        action_to_perform = None
        if args.update:
            action_to_perform = 'update'
        elif args.undo:
            action_to_perform = 'undo'
        elif args.test_and_enable_disabled:
            action_to_perform = 'test_and_enable'
        # find_key mode is handled separately below, not in this interactive menu for single_site
        elif is_interactive_mode and operation_mode == 'single_site': # 纯交互模式下的菜单 (仅对 single_site)
            config_name = Path(connection_config_path_str).stem
            latest_undo_file = find_latest_undo_file_for(config_name) # find_latest_undo_file_for 在 undo_utils

            if latest_undo_file:
                print(f"\n检测到针对 '{config_name}' 的可撤销操作。")
                print(f"撤销文件: {latest_undo_file.name}")
                undo_summary = get_undo_summary(latest_undo_file) # get_undo_summary 在 undo_utils
                print(f"摘要: {undo_summary or '无法获取上次操作的详细信息。'}")

                while True:
                    try:
                        choice = input("请选择操作: [1] 根据 query_config.yaml 查询渠道 [2] 根据 update_config.yaml 更新渠道 [3] 撤销上次操作 (使用 undo_...json) [4] 测试并启用自动禁用的渠道 [5] 根据 channel_model_test_config.yaml 测试模型 [0] 退出: ")
                        if choice == '1': action_to_perform = 'query_all'; break
                        elif choice == '2': action_to_perform = 'update'; break
                        elif choice == '3': action_to_perform = 'undo'; break
                        elif choice == '4': action_to_perform = 'test_and_enable'; break # 动作名不变
                        elif choice == '5': action_to_perform = 'test_channel_model'; break # 动作名不变
                        elif choice == '0': action_to_perform = None; break
                        else: print("无效选项，请输入 0 到 5 之间的数字。")
                    except EOFError: print("\n操作已取消。"); action_to_perform = None; break
            else:
                logging.info(f"未找到针对 '{config_name}' 的撤销文件。")
                # 在没有撤销文件时，提供查询和更新选项
                while True:
                    try:
                        choice = input("请选择操作: [1] 根据 query_config.yaml 查询渠道 [2] 根据 update_config.yaml 更新渠道 [3] 测试并启用自动禁用的渠道 [4] 根据 channel_model_test_config.yaml 测试模型 [0] 退出: ")
                        if choice == '1': action_to_perform = 'query_all'; break
                        elif choice == '2': action_to_perform = 'update'; break
                        elif choice == '3': action_to_perform = 'test_and_enable'; break # 动作名不变
                        elif choice == '4': action_to_perform = 'test_channel_model'; break # 动作名不变
                        elif choice == '0': action_to_perform = None; break
                        else: print("无效选项，请输入 0 到 4 之间的数字。")
                    except EOFError: print("\n操作已取消。"); action_to_perform = None; break

        # --- 执行单站操作 ---
        if action_to_perform == 'query_all':
            print("\n--- 查询所有渠道 ---")
            logging.info("开始查询所有渠道...")
            tool_instance = _get_tool_instance(connection_config_path_str, None, script_config=script_config)
            if not tool_instance:
                final_exit_code = 1
            else:
                final_exit_code = await _execute_query(tool_instance)
        elif action_to_perform == 'update':
            # 传递 script_config 给处理器
            final_exit_code = await run_single_site_operation(args, connection_config_path_str, script_config)
        elif action_to_perform == 'undo':
            config_name = Path(connection_config_path_str).stem
            undo_file_to_use = find_latest_undo_file_for(config_name)
            if not undo_file_to_use:
                 logging.error(f"错误：在执行撤销前未能找到针对 '{config_name}' 的撤销文件。")
                 print(f"错误：未找到可用于 '{config_name}' 的撤销文件。")
                 final_exit_code = 1
            else:
                print(f"\n--- 撤销模式 ---")
                print(f"将使用文件 '{undo_file_to_use.name}' 进行撤销。")
                # 传递 script_config 给处理器
                final_exit_code = await perform_undo(connection_config_path_str, undo_file_to_use, args.yes)
        elif action_to_perform == 'test_and_enable':
            # 传递 script_config 给处理器
            final_exit_code = await run_test_and_enable_disabled(args, connection_config_path_str, script_config)
        elif action_to_perform == 'test_channel_model':
            logging.info("用户选择交互式测试指定模型渠道。")
            # 交互模式下，默认使用根目录的 DEFAULT_CHANNEL_MODEL_TEST_CONFIG_PATH
            # args.test_channel_model 在交互模式下通常为 None
            # 我们需要检查该文件是否存在
            if not DEFAULT_CHANNEL_MODEL_TEST_CONFIG_PATH.is_file():
                print(f"\n错误：测试指定模型功能需要 '{DEFAULT_CHANNEL_MODEL_TEST_CONFIG_NAME}' 文件存在于项目根目录。")
                print(f"请先创建并配置该文件，或使用 '--test-channel-model <路径>' 命令行参数指定自定义路径。")
                logging.error(f"交互模式下执行测试指定模型渠道失败: {DEFAULT_CHANNEL_MODEL_TEST_CONFIG_NAME} 文件不存在。")
                final_exit_code = 1
            else:
                # 调用已有的处理函数，传递默认配置文件路径
                # 注意: args 对象是从命令行解析的，交互模式下其 .test_channel_model 通常为 None
                # run_test_model_on_channels 函数签名已更新，需要传递交互模式下选定的连接配置
                # connection_config_path_str 在此作用域内是用户选择的连接配置
                final_exit_code = await run_test_model_on_channels(
                    args,
                    script_config,
                    str(DEFAULT_CHANNEL_MODEL_TEST_CONFIG_PATH),
                    connection_config_path_str  # 传递交互模式下选定的连接配置
                )
        else: # action_to_perform is None (用户选择退出)
             # 只有在不是因为不支持而退出时才打印取消消息 # (逻辑调整：如果final_exit_code非0，可能是之前的错误)
             if action_to_perform is None and final_exit_code == 0: # 仅在明确退出且无错误时打印取消
                 print("操作已取消。") # 简化消息
             final_exit_code = 0 # 保证正常退出
    
    elif operation_mode == 'test_channel_model': # 新增的操作模式处理
        logging.info("进入测试指定模型渠道流程...")
        # args.test_channel_model 包含配置文件的路径，已在前面验证过存在性
        test_config_path_str = args.test_channel_model
        
        # 调用新的处理函数
        # 注意：run_test_model_on_channels 将需要访问 args (例如 -y) 和 script_config
        final_exit_code = await run_test_model_on_channels(args, script_config, test_config_path_str)
        logging.info(f"测试指定模型渠道操作完成，退出码: {final_exit_code}")

    elif operation_mode == 'query':
        logging.info("进入非交互式查询流程...")
        connection_config_path_str = args.connection_config # Already validated
        try:
            api_config = load_yaml_config(connection_config_path_str)
            if not api_config:
                 print(f"错误：无法加载连接配置文件 '{Path(connection_config_path_str).name}'。")
                 return 1
            api_type = "newapi"
        except Exception as e:
            logging.error(f"加载连接配置 '{connection_config_path_str}' 以获取 API 类型时出错: {e}", exc_info=True)
            print(f"错误：无法从 '{Path(connection_config_path_str).name}' 加载 API 类型。请检查文件和日志。")
            return 1
        
        print(f"\n--- 查询模式 ({Path(connection_config_path_str).name}) ---")
        tool_instance = _get_tool_instance(connection_config_path_str, None, script_config=script_config)
        if not tool_instance:
            final_exit_code = 1
        else:
            final_exit_code = await _execute_query(tool_instance)
        
    elif operation_mode == 'find_key':
        logging.info("进入查找 API Key 流程...")
        key_to_find = args.find_key
        connection_config_path_str = args.connection_config # Already validated

        try:
            api_config = load_yaml_config(connection_config_path_str)
            if not api_config:
                 print(f"错误：无法加载连接配置文件 '{Path(connection_config_path_str).name}'。")
                 return 1
            api_type = "newapi"
            logging.info(f"从配置 '{Path(connection_config_path_str).name}' 加载 API 类型: {api_type} 用于查找 Key。")
        except Exception as e:
            logging.error(f"加载连接配置 '{connection_config_path_str}' 以获取 API 类型时出错: {e}", exc_info=True)
            print(f"错误：无法从 '{Path(connection_config_path_str).name}' 加载 API 类型。请检查文件和日志。")
            return 1

        print(f"\n--- 正在实例 '{Path(connection_config_path_str).name}' ({api_type}) 中查找 API Key: '{key_to_find}' ---")
        tool_instance = _get_tool_instance(connection_config_path_str, None, script_config=script_config)
        if not tool_instance:
            final_exit_code = 1
        else:
            channel_list, msg = await tool_instance.get_all_channels()
            if channel_list is None:
                logging.error(f"获取渠道列表失败: {msg}")
                print(f"错误：获取渠道列表失败。详情请查看日志。")
                final_exit_code = 1
            else:
                found_channels = []
                for channel in channel_list:
                    # 优先检查 'key' 字段，然后是 'apikey'
                    channel_key = channel.get('key')
                    if channel_key is None: # 如果 'key' 不存在或为 None，尝试 'apikey'
                        channel_key = channel.get('apikey')

                    if channel_key == key_to_find:
                        found_channels.append(channel)
                
                if found_channels:
                    print(f"\n找到 {len(found_channels)} 个渠道的 API Key匹配 '{key_to_find}':")
                    for idx, channel_data in enumerate(found_channels):
                        print(f"\n--- 匹配渠道 #{idx + 1} ---")
                        print(json.dumps(channel_data, indent=2, ensure_ascii=False))
                    final_exit_code = 0
                else:
                    print(f"\n在实例 '{Path(connection_config_path_str).name}' 中未找到 API Key 为 '{key_to_find}' 的渠道。")
                    final_exit_code = 0 # 未找到不算错误
        logging.info(f"查找 API Key 操作完成，退出码: {final_exit_code}")

    # --- 替换跨站点逻辑 ---
    elif operation_mode == 'cross_site':
        logging.info("进入跨站操作流程...")
        print("\n--- 跨站点操作 ---")
        print(f"将根据配置文件 '{CROSS_SITE_ACTION_CONFIG_PATH}' 执行操作。") # Inform user

        # 1. 加载跨站点动作配置文件
        cross_site_config = load_yaml_config(CROSS_SITE_ACTION_CONFIG_PATH)
        if not cross_site_config:
            logging.error(f"错误：跨站点动作配置文件 '{CROSS_SITE_ACTION_CONFIG_PATH}' 加载失败或为空。")
            print(f"错误：无法加载或解析 '{CROSS_SITE_ACTION_CONFIG_PATH}'。请检查文件是否存在且内容有效。")
            return 1 # Exit with error

        # 2. 从配置中提取必要参数
        try:
            cross_site_action = cross_site_config.get('action')
            source_config = cross_site_config.get('source', {})
            target_config = cross_site_config.get('target', {})

            source_config_path_str = source_config.get('connection_config')
            target_config_path_str = target_config.get('connection_config')

            # Basic validation for action and connection paths
            if not cross_site_action or not source_config_path_str or not target_config_path_str:
                raise ValueError("配置文件中缺少必要的键 (action, source.connection_config, target.connection_config)")

            # Validate action type
            allowed_actions = ["compare_channel_counts", "compare_fields", "copy_fields"]
            if cross_site_action not in allowed_actions:
                raise ValueError(f"配置文件中的 action ('{cross_site_action}') 不是支持的操作 ({', '.join(allowed_actions)})。")

            # Validate and get filters *only if* required by the action
            source_filter = None
            target_filter = None
            if cross_site_action in ["compare_fields", "copy_fields"]:
                source_filter = source_config.get('channel_filter')
                target_filter = target_config.get('channel_filter')
                if not source_filter or not target_filter:
                    raise ValueError(f"操作 '{cross_site_action}' 需要在配置文件中定义 'source.channel_filter' 和 'target.channel_filter'")
                if not isinstance(source_filter, dict) or not isinstance(target_filter, dict):
                    raise ValueError("配置文件中的 source.channel_filter 和 target.channel_filter 必须是字典。")

            # Check if connection config files exist
            if not Path(source_config_path_str).is_file():
                 raise FileNotFoundError(f"源连接配置文件未找到: {source_config_path_str}")
            if not Path(target_config_path_str).is_file():
                 raise FileNotFoundError(f"目标连接配置文件未找到: {target_config_path_str}")


            logging.info(f"从配置文件加载的操作: {cross_site_action}")
            logging.info(f"从配置文件加载的操作: {cross_site_action}")
            logging.info(f"源配置: {source_config_path_str}" + (f", 源筛选器: {source_filter}" if source_filter else ""))
            logging.info(f"目标配置: {target_config_path_str}" + (f", 目标筛选器: {target_filter}" if target_filter else ""))
            print(f"检测到操作: {cross_site_action}")
            print(f"源: {source_config_path_str}" + (f" (筛选器: {json.dumps(source_filter)})" if source_filter else ""))
            print(f"目标: {target_config_path_str}" + (f" (筛选器: {json.dumps(target_filter)})" if target_filter else ""))

        except (KeyError, ValueError, TypeError, FileNotFoundError) as e:
            logging.error(f"解析跨站点动作配置文件 '{CROSS_SITE_ACTION_CONFIG_PATH}' 时出错: {e}")
            print(f"错误：解析配置文件 '{CROSS_SITE_ACTION_CONFIG_PATH}' 失败。请确保结构正确、包含所有必需键、action有效且文件路径有效。")
            print(f"错误详情: {e}")
            return 1
        except Exception as e: # Catch any other unexpected error during config parsing
            logging.error(f"加载或解析跨站点动作配置文件 '{CROSS_SITE_ACTION_CONFIG_PATH}' 时发生未知错误: {e}", exc_info=True)
            print(f"错误：加载或解析跨站点动作配置文件时发生未知错误。请检查日志。")
            return 1


        # 3. 提示用户确认 (除非 -y)
        if not args.yes:
             try:
                 confirm = input("确认要根据以上配置执行跨站点操作吗? (y/n): ").lower()
                 if confirm != 'y':
                     print("操作已取消。")
                     logging.info("用户取消了跨站点操作。")
                     return 0
                 logging.info("用户确认执行跨站点操作。")
             except EOFError:
                 print("\n操作已取消。")
                 logging.info("用户通过 EOF 取消了跨站点操作。")
                 return 0

        # 4. 调用 cross_site_handler 中的函数
        logging.info("开始调用 run_cross_site_operation...")
        final_exit_code = await run_cross_site_operation(
            args=args,
            action=cross_site_action,
            script_config=script_config
        )
    # --- 结束替换 ---
    else: # operation_mode is None (用户在模式选择时退出)
         final_exit_code = 0

    return final_exit_code
