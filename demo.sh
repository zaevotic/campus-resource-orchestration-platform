#!/usr/bin/env bash
# ==============================================================================
# Campus Resource Orchestration Platform — All-in-One Live Demonstration Script
# ==============================================================================
# Usage:
#   ./demo.sh                 -> Interactive menu for your live presentation
#   ./demo.sh start           -> Start server (auto-detects Docker Kafka/Redis, falls back to local)
#   ./demo.sh start-local     -> Start server in fast zero-Docker mode (SQLite + DuckDB + In-Memory Kafka/Redis)
#   ./demo.sh start-docker    -> Start Docker (Kafka + Kafka UI + Redis + Redis UI) + start server
#   ./demo.sh walkthrough     -> Step-by-step interactive live presenter walkthrough (Stages 1 to 5)
#   ./demo.sh auto            -> Run all 5 live demo stages automatically
#   ./demo.sh seed            -> Seed sample bookings/tickets so Web UI charts look rich
#   ./demo.sh race            -> Fire 30 simultaneous booking requests at the exact same millisecond
#   ./demo.sh chaos           -> Trigger live Notification failure -> Retry -> DLQ -> Replay
#   ./demo.sh benchmark       -> Run the 77-assertion Concurrency & Pipeline Benchmark (--quick)
#   ./demo.sh benchmark-full  -> Run the full 90s Concurrency & Pipeline Benchmark
#   ./demo.sh status          -> Show server, Docker, and health status
#   ./demo.sh stop            -> Stop the running server (and Docker containers if running)
#   ./demo.sh clean           -> Stop everything and reset local demo DBs for a fresh start
# ==============================================================================

set -e

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

PORT="${PORT:-8000}"
PID_FILE="/tmp/campus_orchestrator_demo.pid"
LOG_FILE="/tmp/campus_orchestrator_demo.log"

# Colors
B="\033[1m"
G="\033[92m"
Y="\033[93m"
R="\033[91m"
C="\033[96m"
END="\033[0m"

# ------------------------------------------------------------------------------
# Python & PYTHONPATH resolution (works with local .venv OR ../dir/.venv)
# ------------------------------------------------------------------------------
setup_python_env() {
    PY="$(command -v python3)"
    SP=""
    if [ -d "$HERE/.venv/lib" ]; then
        SP="$(find "$HERE/.venv/lib" -maxdepth 2 -name site-packages | head -1)"
    fi
    if [ -z "$SP" ] && [ -d "$HERE/../dir/.venv/lib" ]; then
        SP="$(find "$HERE/../dir/.venv/lib" -maxdepth 2 -name site-packages | head -1)"
    fi
    if [ -x "$HERE/.venv/bin/python" ] && "$HERE/.venv/bin/python" -c "import fastapi, uvicorn, duckdb, fastavro, httpx" >/dev/null 2>&1; then
        PY="$HERE/.venv/bin/python"
    fi
    if [ -n "$SP" ]; then
        export PYTHONPATH="$SP:$HERE:${PYTHONPATH:-}"
    else
        export PYTHONPATH="$HERE:${PYTHONPATH:-}"
    fi
}

ensure_docker_deps() {
    if ! "$PY" -c "import aiokafka, redis" >/dev/null 2>&1; then
        echo -e "${Y}Installing aiokafka & redis for real Docker Kafka/Redis mode...${END}"
        if command -v uv >/dev/null 2>&1; then
            [ -d "$HERE/.venv" ] || uv venv "$HERE/.venv" --python "$PY"
            uv pip install --python "$HERE/.venv/bin/python" -r "$HERE/requirements.txt"
            PY="$HERE/.venv/bin/python"
        else
            "$PY" -m pip install --user aiokafka redis
        fi
    fi
}

# Avoid DuckDB file-lock conflicts if another terminal already has analytics.duckdb open
prepare_db_paths() {
    if ! "$PY" -c "import duckdb; c=duckdb.connect('$HERE/analytics.duckdb'); c.close()" >/dev/null 2>&1; then
        echo -e "${Y}Note: analytics.duckdb is locked by another process; using isolated demo DBs in /tmp.${END}"
        export SQLITE_PATH="/tmp/campus_demo_live.db"
        export DUCKDB_PATH="/tmp/campus_demo_live.duckdb"
    fi
    # Pick a free port if $PORT is already bound in another namespace
    if ! "$PY" -c "import socket; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('127.0.0.1', int('$PORT'))); s.close()" >/dev/null 2>&1; then
        PORT="$("$PY" -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")"
        export PORT
    fi
}

is_server_running() {
    curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1
}

wait_for_server() {
    echo -ne "${C}Waiting for server on http://localhost:${PORT}/health ...${END}"
    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
            echo -e " ${G}ONLINE!${END}"
            return 0
        fi
        echo -n "."
        sleep 0.5
    done
    echo -e "\n${R}Server failed to start within 30s. Check logs: $LOG_FILE${END}"
    tail -n 30 "$LOG_FILE" || true
    exit 1
}

print_dashboard_info() {
    echo -e "\n${B}${C}================================================================================${END}"
    echo -e "${B}${G}  CAMPUS RESOURCE ORCHESTRATION PLATFORM IS LIVE!${END}"
    echo -e "${B}${C}================================================================================${END}"
    echo -e "  Main Web UI (Dashboard) : http://localhost:${PORT}"
    echo -e "  Swagger API Docs        : http://localhost:${PORT}/docs"
    echo -e "  Health & Metrics JSON   : http://localhost:${PORT}/health"
    if [ "${USE_REAL_KAFKA:-false}" = "true" ]; then
        echo -e "  Kafka UI (Topics/Avro)  : http://localhost:8082"
        echo -e "  RedisInsight UI (KV)    : http://localhost:5540  (Host: 'redis', Port: 6379)"
    fi
    echo -e "--------------------------------------------------------------------------------"
    echo -e "  Demo Accounts (or click the 1-click cards on the login page):"
    echo -e "     - Student        : ${B}student1${END}       / ${B}student123${END}"
    echo -e "     - Faculty        : ${B}faculty1${END}       / ${B}faculty123${END}"
    echo -e "     - Facility Admin : ${B}admin_facility${END} / ${B}facility123${END}"
    echo -e "     - System Admin   : ${B}admin_sys${END}      / ${B}sysadmin123${END}  (Chaos & DLQ tab)"
    echo -e "${B}${C}================================================================================${END}\n"
}

start_local() {
    setup_python_env
    if is_server_running; then
        echo -e "${G}Server is already running on port ${PORT}.${END}"
        print_dashboard_info
        return 0
    fi
    prepare_db_paths
    export USE_REAL_KAFKA=false
    export USE_REAL_REDIS=false
    echo -e "${C}Starting FastAPI + Web UI in local mode on port ${PORT}...${END}"
    nohup "$PY" -m uvicorn src.app:app --host 0.0.0.0 --port "$PORT" >"$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    wait_for_server
    print_dashboard_info
}

start_docker() {
    setup_python_env
    ensure_docker_deps
    echo -e "${C}Starting Docker containers (Kafka, Kafka UI, Redis, RedisInsight)...${END}"
    docker compose up -d
    echo -ne "${C}Waiting for Kafka broker on localhost:9092 ...${END}"
    for _ in $(seq 1 40); do
        if "$PY" -c "import socket; s=socket.create_connection(('127.0.0.1',9092),1); s.close()" 2>/dev/null; then
            echo -e " ${G}READY!${END}"
            break
        fi
        echo -n "."
        sleep 1
    done
    if is_server_running; then
        echo -e "${G}Using running server on port ${PORT}.${END}"
        print_dashboard_info
        return 0
    fi
    prepare_db_paths
    export USE_REAL_KAFKA=true
    export USE_REAL_REDIS=true
    echo -e "${C}Starting FastAPI + Web UI connected to real Kafka & Redis on port ${PORT}...${END}"
    nohup "$PY" -m uvicorn src.app:app --host 0.0.0.0 --port "$PORT" >"$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    wait_for_server
    print_dashboard_info
}

start_auto() {
    setup_python_env
    if is_server_running; then
        echo -e "${G}Detected existing server running at http://localhost:${PORT}${END}"
        return 0
    fi
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
        start_docker
    else
        echo -e "${Y}Docker daemon not active — starting in fast local mode (SQLite + DuckDB + In-Memory Kafka/Redis).${END}"
        start_local
    fi
}

stop_server_only() {
    if [ -f "$PID_FILE" ]; then
        PID="$(cat "$PID_FILE")"
        if kill -0 "$PID" 2>/dev/null; then
            echo -e "${Y}Stopping server (PID $PID)...${END}"
            kill "$PID" 2>/dev/null || true
            sleep 1
        fi
        rm -f "$PID_FILE"
    fi
    rm -f /tmp/campus_demo_live.db /tmp/campus_demo_live.duckdb /tmp/campus_demo_live.duckdb.wal
}

stop_all() {
    stop_server_only
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
        docker compose stop 2>/dev/null || true
    fi
    echo -e "${G}Stopped demo services.${END}"
}

clean_all() {
    stop_all
    rm -f "$HERE/campus_local.db" "$HERE/analytics.duckdb" "$HERE/analytics.duckdb.wal" "$LOG_FILE"
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
        docker compose down -v 2>/dev/null || true
    fi
    echo -e "${G}Cleaned local databases and Docker volumes.${END}"
}

show_status() {
    if is_server_running; then
        echo -e "${G}[ONLINE] Server is running at http://localhost:${PORT}${END}"
        curl -s "http://127.0.0.1:${PORT}/health" | python3 -m json.tool
    else
        echo -e "${R}[OFFLINE] Server is not running${END}"
    fi
}

ensure_running() {
    setup_python_env
    if ! is_server_running; then
        start_auto
    fi
}

interactive_menu() {
    setup_python_env
    while true; do
        echo -e "\n${B}${C}================================================================================${END}"
        echo -e "${B}${C}   CAMPUS RESOURCE ORCHESTRATION — LIVE DEMO CONTROL CENTER${END}"
        echo -e "${B}${C}================================================================================${END}"
        if is_server_running; then
            echo -e "  Server Status: ${G}[ONLINE] (http://localhost:${PORT})${END}"
        else
            echo -e "  Server Status: ${Y}[OFFLINE]${END}"
        fi
        echo -e "--------------------------------------------------------------------------------"
        echo -e "  ${B}1)${END} Start Platform (Local Fast Mode — SQLite + DuckDB + In-Memory Kafka/Redis)"
        echo -e "  ${B}2)${END} Start Platform with Docker (Real Kafka + Kafka UI + Real Redis + Redis UI)"
        echo -e "  ${B}3)${END} Run Interactive Step-by-Step Presenter Walkthrough (Stages 1–5)"
        echo -e "  ${B}4)${END} Seed Sample Bookings & Tickets (Populates Web UI Charts)"
        echo -e "  ${B}5)${END} Trigger 30-Way Simultaneous Double-Booking Race"
        echo -e "  ${B}6)${END} Trigger Chaos Engineering -> Retry -> DLQ -> Replay Demo"
        echo -e "  ${B}7)${END} Run Live Concurrency & Pipeline Benchmark Suite (--quick, 77 checks)"
        echo -e "  ${B}8)${END} Show Live /health & Pipeline Metrics JSON"
        echo -e "  ${B}9)${END} Stop Server / Reset Demo Databases"
        echo -e "  ${B}0)${END} Exit"
        echo -ne "\n${B}Select an option [0-9]: ${END}"
        read -r choice
        case "$choice" in
            1) start_local ;;
            2) start_docker ;;
            3) ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" ;;
            4) ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" --seed-only ;;
            5) ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" --race-only ;;
            6) ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" --chaos-only ;;
            7) "$PY" "$HERE/tests/benchmark_full.py" --quick ;;
            8) show_status ;;
            9) clean_all ;;
            0|q|Q) exit 0 ;;
            *) echo -e "${R}Invalid choice.${END}" ;;
        esac
    done
}

CMD="${1:-menu}"
case "$CMD" in
    start)          start_auto ;;
    start-local)    start_local ;;
    start-docker)   start_docker ;;
    walkthrough)    ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" ;;
    auto)           ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" --auto ;;
    seed)           ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" --seed-only ;;
    race)           ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" --race-only ;;
    chaos)          ensure_running; "$PY" "$HERE/live_demo.py" --url "http://127.0.0.1:${PORT}" --chaos-only ;;
    benchmark)      setup_python_env; "$PY" "$HERE/tests/benchmark_full.py" --quick ;;
    benchmark-full) setup_python_env; "$PY" "$HERE/tests/benchmark_full.py" ;;
    status)         show_status ;;
    stop)           stop_all ;;
    clean)          clean_all ;;
    menu|*)         interactive_menu ;;
esac
