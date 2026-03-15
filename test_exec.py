import asyncio
from src.mesh_agent import MeshAgent

async def test_execute():
    config = MeshAgent.load_config('config.yaml')
    agent = MeshAgent(config)
    await agent.start()
    try:
        # 执行简单计算
        result = await agent.execute_command(
            target_node='laptop-vsgbtehk',
            command='cmd /c echo 1+5-3'
        )
        print('Result:', result)
        if result:
            print('Output:', result.stdout)
            print('Error:', result.stderr)
    finally:
        await agent.stop()

asyncio.run(test_execute())
