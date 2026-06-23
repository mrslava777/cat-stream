#!/usr/bin/env python3
"""TikTok Cat Stream Server — Simple Edition (all files in root)"""

import asyncio
import json
import os
from datetime import datetime
from aiohttp import web

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import GiftEvent, ConnectEvent, DisconnectEvent
    TIKTOK_AVAILABLE = True
except ImportError:
    TIKTOK_AVAILABLE = False

PORT = 8080
HOST = os.getenv("HOST", "0.0.0.0")

connected_clients = {}
gift_history = []
active_tiktok_clients = {}

async def broadcast_gift(gift_data, target_clients=None):
    gift_history.append(gift_data)
    if len(gift_history) > 100:
        gift_history.pop(0)
    message = json.dumps(gift_data)
    clients = target_clients or list(connected_clients.keys())
    disconnected = []
    for client in clients:
        try:
            await client.send_str(message)
        except:
            disconnected.append(client)
    for client in disconnected:
        if client in connected_clients:
            del connected_clients[client]

async def start_tiktok_client(username, websocket):
    if not TIKTOK_AVAILABLE:
        await websocket.send_str(json.dumps({
            "type": "error", "message": "TikTokLive не установлен. DEMO-режим.",
            "timestamp": datetime.now().isoformat()
        }))
        await start_demo_mode(websocket)
        return

    if username in active_tiktok_clients:
        connected_clients[websocket]["username"] = username
        await broadcast_gift({
            "type": "system", "message": f"Подключено к стриму @{username}!",
            "timestamp": datetime.now().isoformat()
        }, [websocket])
        return

    try:
        client = TikTokLiveClient(unique_id=username)
        active_tiktok_clients[username] = client
        connected_clients[websocket]["username"] = username

        @client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            target = [ws for ws, data in connected_clients.items() if data.get("username") == username]
            await broadcast_gift({
                "type": "system", "message": f"Подключено к стриму @{event.unique_id}!",
                "timestamp": datetime.now().isoformat()
            }, target)

        @client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            gift = event.gift
            if gift is None or (hasattr(event, 'streaking') and event.streaking):
                return
            gift_data = {
                "type": "gift",
                "username": event.user.unique_id if hasattr(event.user, 'unique_id') else event.user.nickname,
                "nickname": event.user.nickname if hasattr(event.user, 'nickname') else event.user.unique_id,
                "gift_name": gift.name if hasattr(gift, 'name') else "Unknown",
                "gift_type": getattr(gift, 'type', 0),
                "repeat_count": getattr(event, 'repeat_count', 1),
                "diamond_count": getattr(gift, 'diamond_count', 0) if hasattr(gift, 'diamond_count') else 0,
                "timestamp": datetime.now().isoformat()
            }
            target = [ws for ws, data in connected_clients.items() if data.get("username") == username]
            await broadcast_gift(gift_data, target)

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            target = [ws for ws, data in connected_clients.items() if data.get("username") == username]
            await broadcast_gift({
                "type": "system", "message": "Стрим завершён",
                "timestamp": datetime.now().isoformat()
            }, target)
            if username in active_tiktok_clients:
                del active_tiktok_clients[username]

        await asyncio.to_thread(client.run)
    except Exception as e:
        await websocket.send_str(json.dumps({
            "type": "error", "message": f"Ошибка: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }))

async def stop_tiktok_client(username):
    subscribers = [ws for ws, data in connected_clients.items() if data.get("username") == username]
    if not subscribers and username in active_tiktok_clients:
        try:
            active_tiktok_clients[username].stop()
        except:
            pass
        del active_tiktok_clients[username]

async def start_demo_mode(websocket):
    demo_gifts = [
        {"username": "user123", "nickname": "Анна", "gift_name": "Rose", "repeat_count": 1, "diamond_count": 1},
        {"username": "fan_99", "nickname": "Максим", "gift_name": "Hand Heart", "repeat_count": 1, "diamond_count": 5},
        {"username": "superfan", "nickname": "Катя", "gift_name": "GG", "repeat_count": 3, "diamond_count": 25},
        {"username": "newbie", "nickname": "Иван", "gift_name": "TikTok", "repeat_count": 1, "diamond_count": 1},
        {"username": "rich_guy", "nickname": "Олигарх", "gift_name": "Galaxy", "repeat_count": 1, "diamond_count": 1000},
    ]
    import random
    while websocket in connected_clients and connected_clients[websocket].get("demo"):
        await asyncio.sleep(random.randint(3, 8))
        if websocket not in connected_clients:
            break
        gift = random.choice(demo_gifts)
        gift_data = {
            "type": "gift", "username": gift["username"], "nickname": gift["nickname"],
            "gift_name": gift["gift_name"], "gift_type": 1,
            "repeat_count": gift["repeat_count"], "diamond_count": gift["diamond_count"],
            "timestamp": datetime.now().isoformat()
        }
        try:
            await websocket.send_str(json.dumps(gift_data))
        except:
            break

async def health_handler(request):
    return web.json_response({
        "status": "ok", "clients": len(connected_clients),
        "active_streams": len(active_tiktok_clients)
    })

async def index_handler(request):
    return web.json_response({
        "name": "TikTok Cat Stream", "status": "running",
        "endpoints": {"health": "/health", "websocket": "/ws"}
    })

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients[ws] = {"username": None, "demo": False}

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("action") == "connect":
                    username = data.get("username", "").strip().lstrip("@")
                    if not username:
                        await ws.send_str(json.dumps({"type": "error", "message": "Введите username!"}))
                        continue
                    connected_clients[ws]["username"] = username
                    asyncio.create_task(start_tiktok_client(username, ws))
                elif data.get("action") == "demo":
                    connected_clients[ws]["demo"] = True
                    connected_clients[ws]["username"] = "demo"
                    asyncio.create_task(start_demo_mode(ws))
                    await ws.send_str(json.dumps({"type": "system", "message": "DEMO активирован!"}))
                elif data.get("action") == "disconnect":
                    break
            elif msg.type == web.WSMsgType.ERROR:
                break
    except:
        pass
    finally:
        username = connected_clients.get(ws, {}).get("username")
        if ws in connected_clients:
            del connected_clients[ws]
        if username:
            await stop_tiktok_client(username)
    return ws

async def main():
    app = web.Application()
    app.router.add_get('/', index_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/ws', websocket_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)

    print(f"🐱 Server running on http://{HOST}:{PORT}")
    await site.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
