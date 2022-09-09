from url import url

import tornado.web

application = tornado.web.Application(
    handlers=url,
    debug=True,
    compile_template_cache=False,
    static_hash_cache=False)