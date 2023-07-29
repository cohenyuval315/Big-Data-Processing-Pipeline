def setup_routes(app, handler, project_root):
    router = app.router
    h = handler
    router.add_get('/data', h.data, name='data')
    router.add_get('/predictions', h.predictions, name='predictions')
    router.add_get('/history_data', h.history_data, name='history_data')
    router.add_get('/history_predictions', h.history_predictions, name='history_predictions')
