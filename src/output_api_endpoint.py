from flask import Flask, make_response, jsonify

from postgre_etl.postgre_op import PostgresEtl

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify(status='UP')


@app.route('/users/<string:name>')
def with_url_variables(name: str):
    obj = PostgresEtl()
    data = obj.get_user(name)
    return make_response(jsonify(data), 200)


if __name__ == '__main__':
    app.run(debug=True)
