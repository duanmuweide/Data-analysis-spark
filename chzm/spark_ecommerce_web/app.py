from flask import Flask, jsonify, render_template

from config import Config
from routes.analysis import analysis_bp


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_object(Config)
    app.json.ensure_ascii = False
    app.register_blueprint(analysis_bp, url_prefix="/api")

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            refresh_seconds=Config.REALTIME_REFRESH_SECONDS,
        )

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({"success": False, "message": "访问的页面或接口不存在"}), 404

    @app.errorhandler(Exception)
    def server_error(error):
        app.logger.exception(error)
        return jsonify({"success": False, "message": "服务器查询失败", "detail": str(error)}), 500

    return app


app = create_app()


if __name__ == "__main__":
    app.run(
        host=Config.WEB_HOST,
        port=Config.WEB_PORT,
        debug=Config.FLASK_DEBUG,
    )
