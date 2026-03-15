import asyncio
import sys
sys.path.insert(0, 'src')

from mesh_agent import MeshAgent

async def main():
    config = MeshAgent.load_config('config.yaml')
    agent = MeshAgent(config)
    await agent.start()
    
    # 等待连接建立
    await asyncio.sleep(3)
    
    # 发送消息
    success = await agent.send_message(
        to_node='mark1',
        message_type='command',
        payload={'command': 'echo 1+5-3'}
    )
    
    print(f'Message sent: {success}')
    
    await asyncio.sleep(2)
    await agent.stop()

if __name__ == '__main__':
    asyncio.run(main())
