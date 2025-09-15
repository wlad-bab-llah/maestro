def _setup_dependencies(self, dag: DAG, tasks: List[Any]) -> None:
    """Setup sequential task dependencies"""
    if not hasattr(dag, 'business_tasks') or not dag.business_tasks:
        # No business tasks, connect start to end directly
        dag.start_task >> dag.end_task
        logger.warning("⚠️ No business tasks found, empty pipeline")
        return
    
    # Setup SEQUENTIAL dependencies: start -> task1 -> task2 -> ... -> taskN -> end
    business_tasks = dag.business_tasks
    
    # Connect start task to first business task
    dag.start_task >> business_tasks[0]
    
    # Chain business tasks sequentially
    for i in range(len(business_tasks) - 1):
        business_tasks[i] >> business_tasks[i + 1]
    
    # Connect last business task to end
    business_tasks[-1] >> dag.end_task
    
    logger.info(f"🔗 Sequential dependencies configured: {len(business_tasks)} business tasks in chain")
    logger.info(f"📋 Task execution order: {' -> '.join([task.task_id for task in business_tasks])}")