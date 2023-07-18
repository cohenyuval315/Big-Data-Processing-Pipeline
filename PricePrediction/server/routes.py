def setup_routes(app, handler, project_root):
    router = app.router
    h = handler
    router.add_get('/history', h.history_data, name='history')
    router.add_get('/new_data', h.new_data, name='new_data')
