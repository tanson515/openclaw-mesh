import asyncio
from src.mesh_agent import MeshAgent

async def test():
    config = MeshAgent.load_config('config.yaml')
    agent = MeshAgent(config)
    await agent.start()
    await asyncio.sleep(2)
    
    result = await agent.execute_command(
        target_node='laptop-vsgbtehk',
        command='cmd /c echo 1+5-3'
    )
    
    print('=== RESULT ===')
    print(result)
    if result:
        print('stdout:', result.stdout)
        print('stderr:', result.stderr)
        print('exit_code:', result.exit_code)
    
    await agent.stop()

asyncio.run(test())
