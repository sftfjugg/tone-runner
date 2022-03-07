from flask import Flask, render_template, request
from flask_restful import Api

from web_api.api import QueryDataServer
from web_api.route import RouteHandler


app = Flask(__name__)
api = Api(app)
RouteHandler(api).set_route()


@app.route('/query_all_info')
def query_all_info():
    job_id = request.args.get("job_id")
    data = QueryDataServer().query_data(job_id)
    return render_template('query_info.html', **data)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=7001)
