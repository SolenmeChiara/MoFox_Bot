# 再用这个就写一行注释来混提交的我直接全部🌿飞😡
import asyncio
import signal
import sys
import time
import traceback
from functools import partial
from random import choices
from typing import Any, Callable, Coroutine

from maim_message import MessageServer
from rich.traceback import install

from src.chat.emoji_system.emoji_manager import get_emoji_manager
from src.chat.memory_system.memory_manager import memory_manager
from src.chat.message_receive.bot import chat_bot
from src.chat.message_receive.chat_stream import get_chat_manager
from src.chat.utils.statistic import OnlineTimeRecordTask, StatisticOutputTask
from src.common.logger import get_logger
from src.common.message import get_global_api
from src.common.remote import TelemetryHeartBeatTask
from src.common.server import Server, get_global_server
from src.config.config import global_config
from src.individuality.individuality import Individuality, get_individuality
from src.manager.async_task_manager import async_task_manager
from src.mood.mood_manager import mood_manager
from src.plugin_system.base.component_types import EventType
from src.plugin_system.base.base_interest_calculator import BaseInterestCalculator
from src.plugin_system.core.event_manager import event_manager
from src.plugin_system.core.plugin_manager import plugin_manager
from src.schedule.monthly_plan_manager import monthly_plan_manager
from src.schedule.schedule_manager import schedule_manager

# 插件系统现在使用统一的插件加载器
install(extra_lines=3)

logger = get_logger("main")

# 预定义彩蛋短语，避免在每次初始化时重新创建
EGG_PHRASES: list[tuple[str, int]] = [
    ("我们的代码里真的没有bug，只有'特性'。", 10),
    ("你知道吗？阿范喜欢被切成臊子😡", 10),
    ("你知道吗,雅诺狐的耳朵其实很好摸", 5),
    ("你群最高技术力————言柒姐姐！", 20),
    ("初墨小姐宇宙第一(不是)", 10),
    ("world.execute(me);", 10),
    ("正在尝试连接到MaiBot的服务器...连接失败...，正在转接到maimaiDX", 10),
    ("你的bug就像星星一样多，而我的代码像太阳一样，一出来就看不见了。", 10),
    ("温馨提示：请不要在代码中留下任何魔法数字，除非你知道它的含义。", 10),
    ("世界上只有10种人：懂二进制的和不懂的。", 10),
    ("喵喵~你的麦麦被猫娘入侵了喵~", 15),
    ("恭喜你触发了稀有彩蛋喵：诺狐嗷呜~ ~", 1),
    ("恭喜你！！！你的开发者模式已成功开启，快来加入我们吧！(๑•̀ㅂ•́)و✧   (小声bb:其实是当黑奴)", 10),
]


def _task_done_callback(task: asyncio.Task, message_id: str, start_time: float) -> None:
    """后台任务完成时的回调函数"""
    end_time = time.time()
    duration = end_time - start_time
    try:
        task.result()  # 如果任务有异常，这里会重新抛出
        logger.debug(f"消息 {message_id} 的后台任务 (ID: {id(task)}) 已成功完成, 耗时: {duration:.2f}s")
    except asyncio.CancelledError:
        logger.warning(f"消息 {message_id} 的后台任务 (ID: {id(task)}) 被取消, 耗时: {duration:.2f}s")
    except Exception:
        logger.error(f"处理消息 {message_id} 的后台任务 (ID: {id(task)}) 出现未捕获的异常, 耗时: {duration:.2f}s:")
        logger.error(traceback.format_exc())


class MainSystem:
    """主系统类，负责协调所有组件"""

    def __init__(self) -> None:
        # 使用增强记忆系统
        self.memory_manager = memory_manager
        self.individuality: Individuality = get_individuality()

        # 使用消息API替代直接的FastAPI实例
        self.app: MessageServer = get_global_api()
        self.server: Server = get_global_server()

        # 设置信号处理器用于优雅退出
        self._shutting_down = False
        self._setup_signal_handlers()

        # 存储清理任务的引用
        self._cleanup_tasks: list[asyncio.Task] = []

    def _setup_signal_handlers(self) -> None:
        """设置信号处理器"""

        def signal_handler(signum, frame):
            if self._shutting_down:
                logger.warning("系统已经在关闭过程中，忽略重复信号")
                return

            self._shutting_down = True
            logger.info("收到退出信号，正在优雅关闭系统...")

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # 如果事件循环正在运行，创建任务并设置回调
                    async def cleanup_and_exit():
                        await self._async_cleanup()
                        # 给日志系统一点时间刷新
                        await asyncio.sleep(0.1)
                        sys.exit(0)

                    task = asyncio.create_task(cleanup_and_exit())
                    # 存储清理任务引用
                    self._cleanup_tasks.append(task)
                    # 添加任务完成回调，确保程序退出
                    task.add_done_callback(lambda t: sys.exit(0) if not t.cancelled() else None)
                else:
                    # 如果事件循环未运行，使用同步清理
                    self._cleanup()
                    sys.exit(0)
            except Exception as e:
                logger.error(f"信号处理失败: {e}")
                sys.exit(1)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def _initialize_interest_calculator(self) -> None:
        """初始化兴趣值计算组件 - 通过插件系统自动发现和加载"""
        try:
            logger.info("开始自动发现兴趣值计算组件...")

            # 使用组件注册表自动发现兴趣计算器组件
            interest_calculators = {}
            try:
                from src.plugin_system.apis.component_manage_api import get_components_info_by_type
                from src.plugin_system.base.component_types import ComponentType

                interest_calculators = get_components_info_by_type(ComponentType.INTEREST_CALCULATOR)
                logger.info(f"通过组件注册表发现 {len(interest_calculators)} 个兴趣计算器组件")
            except Exception as e:
                logger.error(f"从组件注册表获取兴趣计算器失败: {e}")

            if not interest_calculators:
                logger.warning("未发现任何兴趣计算器组件")
                return

            # 初始化兴趣度管理器
            from src.chat.interest_system.interest_manager import get_interest_manager

            interest_manager = get_interest_manager()
            await interest_manager.initialize()

            # 尝试注册所有可用的计算器
            registered_calculators = []

            for calc_name, calc_info in interest_calculators.items():
                enabled = getattr(calc_info, "enabled", True)
                default_enabled = getattr(calc_info, "enabled_by_default", True)

                if not enabled or not default_enabled:
                    logger.info(f"兴趣计算器 {calc_name} 未启用，跳过")
                    continue

                try:
                    from src.plugin_system.core.component_registry import component_registry

                    component_class = component_registry.get_component_class(
                        calc_name, ComponentType.INTEREST_CALCULATOR
                    )

                    if not component_class:
                        logger.warning(f"无法找到 {calc_name} 的组件类")
                        continue

                    logger.info(f"成功获取 {calc_name} 的组件类: {component_class.__name__}")

                    # 确保组件是 BaseInterestCalculator 的子类
                    if not issubclass(component_class, BaseInterestCalculator):
                        logger.warning(f"{calc_name} 不是 BaseInterestCalculator 的有效子类")
                        continue

                    # 创建组件实例
                    calculator_instance = component_class()

                    # 初始化组件
                    if not await calculator_instance.initialize():
                        logger.error(f"兴趣计算器 {calc_name} 初始化失败")
                        continue

                    # 注册到兴趣管理器
                    if await interest_manager.register_calculator(calculator_instance):
                        registered_calculators.append(calculator_instance)
                        logger.info(f"成功注册兴趣计算器: {calc_name}")
                    else:
                        logger.error(f"兴趣计算器 {calc_name} 注册失败")

                except Exception as e:
                    logger.error(f"处理兴趣计算器 {calc_name} 时出错: {e}", exc_info=True)

            if registered_calculators:
                logger.info(f"成功注册了 {len(registered_calculators)} 个兴趣计算器")
                for calc in registered_calculators:
                    logger.info(f"  - {calc.component_name} v{calc.component_version}")
            else:
                logger.error("未能成功注册任何兴趣计算器")

        except Exception as e:
            logger.error(f"初始化兴趣度计算器失败: {e}", exc_info=True)

    async def _async_cleanup(self) -> None:
        """异步清理资源"""
        if self._shutting_down:
            return

        self._shutting_down = True
        logger.info("开始系统清理流程...")

        cleanup_tasks = []

        # 停止数据库服务
        try:
            from src.common.database.database import stop_database

            cleanup_tasks.append(("数据库服务", stop_database()))
        except Exception as e:
            logger.error(f"准备停止数据库服务时出错: {e}")

        # 停止消息管理器
        try:
            from src.chat.message_manager import message_manager

            cleanup_tasks.append(("消息管理器", message_manager.stop()))
        except Exception as e:
            logger.error(f"准备停止消息管理器时出错: {e}")

        # 停止消息重组器
        try:
            from src.utils.message_chunker import reassembler

            cleanup_tasks.append(("消息重组器", reassembler.stop_cleanup_task()))
        except Exception as e:
            logger.error(f"准备停止消息重组器时出错: {e}")

        # 停止增强记忆系统
        try:
            if global_config.memory.enable_memory:
                cleanup_tasks.append(("增强记忆系统", self.memory_manager.shutdown()))
        except Exception as e:
            logger.error(f"准备停止增强记忆系统时出错: {e}")

        # 触发停止事件
        try:
            from src.plugin_system.core.event_manager import event_manager

            cleanup_tasks.append(
                ("插件系统停止事件", event_manager.trigger_event(EventType.ON_STOP, permission_group="SYSTEM"))
            )
        except Exception as e:
            logger.error(f"准备触发停止事件时出错: {e}")

        # 停止表情管理器
        try:
            cleanup_tasks.append(
                ("表情管理器", asyncio.get_event_loop().run_in_executor(None, get_emoji_manager().shutdown))
            )
        except Exception as e:
            logger.error(f"准备停止表情管理器时出错: {e}")

        # 停止服务器
        try:
            if self.server:
                cleanup_tasks.append(("服务器", self.server.shutdown()))
        except Exception as e:
            logger.error(f"准备停止服务器时出错: {e}")

        # 停止应用
        try:
            if self.app:
                if hasattr(self.app, "stop"):
                    cleanup_tasks.append(("应用", self.app.stop()))
        except Exception as e:
            logger.error(f"准备停止应用时出错: {e}")

        # 并行执行所有清理任务
        if cleanup_tasks:
            logger.info(f"开始并行执行 {len(cleanup_tasks)} 个清理任务...")
            tasks = [task for _, task in cleanup_tasks]
            task_names = [name for name, _ in cleanup_tasks]

            # 使用asyncio.gather并行执行，设置超时防止卡死
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=30.0,  # 30秒超时
                )

                # 记录结果
                for i, (name, result) in enumerate(zip(task_names, results)):
                    if isinstance(result, Exception):
                        logger.error(f"停止 {name} 时出错: {result}")
                    else:
                        logger.info(f"🛑 {name} 已停止")

            except asyncio.TimeoutError:
                logger.error("清理任务超时，强制退出")
            except Exception as e:
                logger.error(f"执行清理任务时发生错误: {e}")
        else:
            logger.warning("没有需要清理的任务")

    def _cleanup(self) -> None:
        """同步清理资源（向后兼容）"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果循环正在运行，创建异步清理任务
                task = asyncio.create_task(self._async_cleanup())
                self._cleanup_tasks.append(task)
            else:
                # 如果循环未运行，直接运行异步清理
                loop.run_until_complete(self._async_cleanup())
        except Exception as e:
            logger.error(f"同步清理资源时出错: {e}")

    async def _message_process_wrapper(self, message_data: dict[str, Any]) -> None:
        """并行处理消息的包装器"""
        try:
            start_time = time.time()
            message_id = message_data.get("message_info", {}).get("message_id", "UNKNOWN")

            # DEBUG: 记录所有收到的消息
            message_segment = message_data.get("message_segment")
            if message_segment:
                msg_type = message_segment.get("type") if isinstance(message_segment, dict) else getattr(message_segment, "type", "UNKNOWN")
                logger.info(f"[DEBUG main.py] 收到消息: message_id={message_id}, type={msg_type}")
            else:
                logger.info(f"[DEBUG main.py] 收到消息: message_id={message_id}, 无message_segment")

            # 检查系统是否正在关闭
            if self._shutting_down:
                logger.warning(f"系统正在关闭，拒绝处理消息 {message_id}")
                return

            # 创建后台任务
            task = asyncio.create_task(chat_bot.message_process(message_data))
            logger.debug(f"已为消息 {message_id} 创建后台处理任务 (ID: {id(task)})")

            # 添加一个回调函数，当任务完成时，它会被调用
            task.add_done_callback(partial(_task_done_callback, message_id=message_id, start_time=start_time))
        except Exception:
            logger.error("在创建消息处理任务时发生严重错误:")
            logger.error(traceback.format_exc())

    async def initialize(self) -> None:
        """初始化系统组件"""
        # 检查必要的配置
        if not hasattr(global_config, "bot") or not hasattr(global_config.bot, "nickname"):
            logger.error("缺少必要的bot配置")
            raise ValueError("Bot配置不完整")

        logger.info(f"正在唤醒{global_config.bot.nickname}......")

        # 初始化组件
        await self._init_components()

        # 随机选择彩蛋
        egg_texts, weights = zip(*EGG_PHRASES)
        selected_egg = choices(egg_texts, weights=weights, k=1)[0]

        logger.info(f"""
全部系统初始化完成，{global_config.bot.nickname}已成功唤醒
=========================================================
MoFox_Bot(第三方修改版)
全部组件已成功启动!
=========================================================
🌐 项目地址: https://github.com/MoFox-Studio/MoFox_Bot
🏠 官方项目: https://github.com/MaiM-with-u/MaiBot
=========================================================
这是基于原版MMC的社区改版，包含增强功能和优化(同时也有更多的'特性')
=========================================================
小贴士:{selected_egg}
""")

    async def _init_components(self) -> None:
        """初始化其他组件"""
        init_start_time = time.time()

        # 并行初始化基础组件
        base_init_tasks = [
            async_task_manager.add_task(OnlineTimeRecordTask()),
            async_task_manager.add_task(StatisticOutputTask()),
            async_task_manager.add_task(TelemetryHeartBeatTask()),
        ]

        await asyncio.gather(*base_init_tasks, return_exceptions=True)
        logger.info("基础定时任务初始化成功")

        # 注册默认事件
        event_manager.init_default_events()

        # 初始化权限管理器
        try:
            from src.plugin_system.apis.permission_api import permission_api
            from src.plugin_system.core.permission_manager import PermissionManager

            permission_manager = PermissionManager()
            await permission_manager.initialize()
            permission_api.set_permission_manager(permission_manager)
            logger.info("权限管理器初始化成功")
        except Exception as e:
            logger.error(f"权限管理器初始化失败: {e}")

        # 注册API路由
        try:
            from src.api.message_router import router as message_router
            from src.api.statistic_router import router as llm_statistic_router

            self.server.register_router(message_router, prefix="/api")
            self.server.register_router(llm_statistic_router, prefix="/api")
            logger.info("API路由注册成功")
        except Exception as e:
            logger.error(f"注册API路由失败: {e}")

        # 加载所有插件
        plugin_manager.load_all_plugins()

        # 处理所有缓存的事件订阅（插件加载完成后）
        event_manager.process_all_pending_subscriptions()
        
        # 初始化表情管理器
        get_emoji_manager().initialize()
        logger.info("表情包管理器初始化成功")

        """
        # 初始化回复后关系追踪系统
        try:
            from src.plugins.built_in.affinity_flow_chatter.interest_scoring import chatter_interest_scoring_system
            from src.plugins.built_in.affinity_flow_chatter.relationship_tracker import ChatterRelationshipTracker

            relationship_tracker = ChatterRelationshipTracker(interest_scoring_system=chatter_interest_scoring_system)
            chatter_interest_scoring_system.relationship_tracker = relationship_tracker
            logger.info("回复后关系追踪系统初始化成功")
        except Exception as e:
            logger.error(f"回复后关系追踪系统初始化失败: {e}")
            relationship_tracker = None
        """

        # 启动情绪管理器
        await mood_manager.start()
        logger.info("情绪管理器初始化成功")

        # 启动聊天管理器的自动保存任务
        asyncio.create_task(get_chat_manager()._auto_save_task())

        # 初始化增强记忆系统
        if global_config.memory.enable_memory:
            from src.chat.memory_system.memory_system import initialize_memory_system
            await self._safe_init("增强记忆系统", initialize_memory_system)()
            await self._safe_init("记忆管理器", self.memory_manager.initialize)()
        else:
            logger.info("记忆系统已禁用，跳过初始化")

        # 初始化消息兴趣值计算组件
        await self._initialize_interest_calculator()

        # 初始化LPMM知识库
        try:
            from src.chat.knowledge.knowledge_lib import initialize_lpmm_knowledge

            initialize_lpmm_knowledge()
            logger.info("LPMM知识库初始化成功")
        except Exception as e:
            logger.error(f"LPMM知识库初始化失败: {e}")

        # 将消息处理函数注册到API
        self.app.register_message_handler(self._message_process_wrapper)

        # 启动消息重组器
        try:
            from src.utils.message_chunker import reassembler

            await reassembler.start_cleanup_task()
            logger.info("消息重组器已启动")
        except Exception as e:
            logger.error(f"启动消息重组器失败: {e}")

        # 启动消息管理器
        try:
            from src.chat.message_manager import message_manager

            await message_manager.start()
            logger.info("消息管理器已启动")
        except Exception as e:
            logger.error(f"启动消息管理器失败: {e}")

        # 初始化个体特征
        await self._safe_init("个体特征", self.individuality.initialize)()

        # 初始化计划相关组件
        await self._init_planning_components()

        # 触发启动事件
        try:
            await event_manager.trigger_event(EventType.ON_START, permission_group="SYSTEM")
            init_time = int(1000 * (time.time() - init_start_time))
            logger.info(f"初始化完成，神经元放电{init_time}次")
        except Exception as e:
            logger.error(f"启动事件触发失败: {e}")

    async def _init_planning_components(self) -> None:
        """初始化计划相关组件"""
        # 初始化月度计划管理器
        if global_config.planning_system.monthly_plan_enable:
            try:
                await monthly_plan_manager.start_monthly_plan_generation()
                logger.info("月度计划管理器初始化成功")
            except Exception as e:
                logger.error(f"月度计划管理器初始化失败: {e}")

        # 初始化日程管理器
        if global_config.planning_system.schedule_enable:
            try:
                await schedule_manager.load_or_generate_today_schedule()
                await schedule_manager.start_daily_schedule_generation()
                logger.info("日程表管理器初始化成功")
            except Exception as e:
                logger.error(f"日程表管理器初始化失败: {e}")

    def _safe_init(self, component_name: str, init_func) -> "Callable[[], Coroutine[Any, Any, bool]]":
        """安全初始化组件，捕获异常"""

        async def wrapper():
            try:
                result = init_func()
                if asyncio.iscoroutine(result):
                    await result
                logger.info(f"{component_name}初始化成功")
                return True
            except Exception as e:
                logger.error(f"{component_name}初始化失败: {e}")
                return False

        return wrapper

    async def schedule_tasks(self) -> None:
        """调度定时任务"""
        try:
            while not self._shutting_down:
                try:
                    tasks = [
                        get_emoji_manager().start_periodic_check_register(),
                        self.app.run(),
                        self.server.run(),
                    ]

                    # 使用 return_exceptions=True 防止单个任务失败导致整个程序崩溃
                    await asyncio.gather(*tasks, return_exceptions=True)

                except (ConnectionResetError, OSError) as e:
                    if self._shutting_down:
                        break
                    logger.warning(f"网络连接发生错误，尝试重新启动任务: {e}")
                    await asyncio.sleep(1)
                except asyncio.InvalidStateError as e:
                    if self._shutting_down:
                        break
                    logger.error(f"异步任务状态无效，重新初始化: {e}")
                    await asyncio.sleep(2)
                except Exception as e:
                    if self._shutting_down:
                        break
                    logger.error(f"调度任务发生未预期异常: {e}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            logger.info("调度任务被取消，正在退出...")
        except Exception as e:
            logger.error(f"调度任务发生致命异常: {e}")
            logger.error(traceback.format_exc())
            raise

    async def shutdown(self) -> None:
        """关闭系统组件"""
        if self._shutting_down:
            return

        logger.info("正在关闭MainSystem...")
        await self._async_cleanup()
        logger.info("MainSystem关闭完成")


async def main() -> None:
    """主函数"""
    system = MainSystem()
    try:
        await system.initialize()
        await system.schedule_tasks()
    except KeyboardInterrupt:
        logger.info("收到键盘中断信号")
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
        logger.error(traceback.format_exc())
    finally:
        await system.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
