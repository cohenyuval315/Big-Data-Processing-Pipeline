def setup_routes(app, handler, project_root):
    router = app.router
    h = handler
    router.add_get('/data', h.data, name='data')

