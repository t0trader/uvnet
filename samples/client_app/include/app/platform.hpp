#pragma once

namespace app::platform {

// Windows 控制台默认不是 UTF-8，这里统一切到 CP_UTF8；Linux 无操作
void init_console();

// 接管 Ctrl+C / SIGTERM：处理函数只置原子标志，收尾在循环线程上做
void install_stop_handler();

// 是否收到过停止信号
bool stop_requested();

} // namespace app::platform
