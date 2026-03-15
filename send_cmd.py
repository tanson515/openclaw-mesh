import asyncio
import sys
sys.path.insert(0, 'src')

from mesh_agent import MeshAgent

async def main():
    config = MeshAgent.load_config('config.yaml')
    agent = MeshAgent(config)
    await agent.start()
    
    # Wait for connection
    await asyncio.sleep(5)
    
    # Send command message
    success = await agent.send_message(
        to_node='schh',
        message_type='command',
        payload={'command': 'echo 1+5-3'}
    )
    
    print(f'Message sent: {success}')
    
    # Wait for response
    await asyncio.sleep(10)
    
    await agent.stop()
    print('Done')

if __name__ == '__main__':
    asyncio.run(main())
