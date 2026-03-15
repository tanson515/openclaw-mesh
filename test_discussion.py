import asyncio
import sys
sys.path.insert(0, 'src')

from mesh_agent import MeshAgent

async def main():
    config = MeshAgent.load_config('config.yaml')
    agent = MeshAgent(config)
    await agent.start()
    
    print(f"节点 {agent.node_id} 已启动")
    print("发起讨论...")
    
    # 发起讨论（mark3 作为协调者）
    did = await agent.start_discussion(
        topic="如何设计一个高并发的消息队列系统？",
        context="当前系统需要处理每秒 10 万条消息",
        participants=["schh", "laptop-vsgbtehk"],  # 根据实际节点名修改
        user_id="user",
        max_rounds=2,
    )
    
    print(f"讨论 ID: {did}")
    print("等待讨论结果（约 2-5 分钟）...")
    
    # 等待结果
    if agent._discussion_coordinator:
        result = await agent._discussion_coordinator.wait(did, timeout=300)
        
        if result:
            print("\n" + "="*50)
            print("讨论结果:")
            print("="*50)
            print(f"主题: {result['topic']}")
            print(f"状态: {result['status']}")
            print(f"\n总结:\n{result['summary']}")
            print(f"\n完整记录:")
            for speech in result['transcript']:
                print(f"\n[{speech['speaker']} 第{speech['round']}轮]:")
                print(f"{speech['content']}")
        else:
            print("讨论超时或失败")
    else:
        print("讨论协调器未初始化")
    
    await agent.stop()

if __name__ == '__main__':
    asyncio.run(main())
