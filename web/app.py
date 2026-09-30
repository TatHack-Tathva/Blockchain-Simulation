"""
    Web layer: a Flask app that talks to one running node through its thread-safe API
    (BasePeer.call_threadsafe). It only reads snapshots and submits requests; every
    transaction goes through the node's normal validation path (the same code that
    handles transactions from peers). No consensus logic lives here.

    Never exposed: private keys, the filesystem, shell / Python / subprocess execution,
    contract deployment (arbitrary code). Only coin transfers can be submitted.
"""
import logging, threading
from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from werkzeug.serving import make_server

from shared_blockchain_structures import ValidationError, is_number

log = logging.getLogger("web")

STATIC_DIR = Path(__file__).resolve().parent / "static"
API_TIMEOUT = 30.0  # seconds a web request waits for the node before answering 504


def create_app(node):
    app = Flask(__name__, static_folder=None)
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024
    app.config["JSON_SORT_KEYS"] = False

    def call(coro_fn, *args):
        return node.call_threadsafe(coro_fn, *args, timeout=API_TIMEOUT)

    def snapshot(what, *args):
        return call(node.api_snapshot, what, *args)

    def body():
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            raise ValidationError("request body must be a JSON object")
        return data

    @app.errorhandler(ValidationError)
    def validation_error(e):
        return jsonify({"ok": False, "error": str(e)}), 400

    @app.errorhandler(FutureTimeout)
    def timeout_error(e):
        return jsonify({"ok": False, "error": "node did not answer in time"}), 504

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"ok": False, "error": "not found"}), 404

    @app.errorhandler(405)
    def not_allowed(e):
        return jsonify({"ok": False, "error": "method not allowed"}), 405

    @app.errorhandler(413)
    def too_large(e):
        return jsonify({"ok": False, "error": "request too large"}), 413

    @app.after_request
    def headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self' 'unsafe-inline'"
        return response

    @app.get("/")
    def dashboard():
        return send_from_directory(STATIC_DIR, "dashboard.html")

    @app.get("/favicon.ico")
    def favicon():
        return "", 204

    @app.get("/static/<path:name>")
    def static_files(name):
        if name not in ("dashboard.js", "dashboard.css"):
            return not_found(None)
        return send_from_directory(STATIC_DIR, name)

    @app.get("/api/node")
    def api_node():
        return jsonify(snapshot("node"))

    @app.get("/api/network")
    def api_network():
        return jsonify(snapshot("network"))

    @app.get("/api/blocks")
    def api_blocks():
        limit = request.args.get("limit", default=50, type=int)
        return jsonify({"blocks": snapshot("blocks", max(1, min(limit or 50, 500)))})

    @app.get("/api/blocks/<int:height>")
    def api_block(height):
        block = snapshot("block", height)
        if block is None:
            return jsonify({"ok": False, "error": "no such block"}), 404
        return jsonify(block)

    @app.get("/api/transactions")
    def api_transactions():
        limit = request.args.get("limit", default=50, type=int)
        return jsonify({"transactions": snapshot("transactions", max(1, min(limit or 50, 500)))})

    @app.post("/api/transactions")
    def api_submit_transaction():
        data = body()
        receiver, amount = data.get("receiver"), data.get("amount")
        if not isinstance(receiver, str) or not receiver.strip():
            raise ValidationError("receiver must be a peer name or public key")
        if receiver.strip().lower() in ("deploy", "invoke"):
            raise ValidationError("only coin transfers can be submitted from the web interface")
        if not is_number(amount):
            raise ValidationError("amount must be a number")
        summary = call(node.api_submit_payment, receiver, amount)
        return jsonify({"ok": True, "transaction": summary}), 201

    @app.post("/api/stake")
    def api_stake():
        if not hasattr(node, "stake"):
            raise ValidationError("staking is only available on PoS nodes")
        amount = body().get("amount")
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise ValidationError("amount must be an integer")
        try:
            stake = call(node.stake, amount)
        except ValidationError as e:
            retry = getattr(e, "retry_after", None)
            return jsonify({"ok": False, "error": str(e), "retry_after": retry}), 409 if retry else 400
        return jsonify({"ok": True, "stake": {"amount": stake.amt, "id": stake.id}}), 201

    def room_action(action):
        from signalling.client import SignallingError
        room = body().get("room")
        if not isinstance(room, str) or not room.strip():
            raise ValidationError("room is required")
        try:
            reply = call(getattr(node, action), room.strip())
        except SignallingError as e:
            status = 409 if e.code in ("room_exists", "already_in_room", "name_taken", "address_in_use") else 400
            return jsonify({"ok": False, "error": e.message, "code": e.code}), status
        return jsonify({"ok": True, "room": reply.get("room"), "members": len(reply.get("members", []))}), 200

    @app.post("/api/rooms/create")
    def api_create_room():
        return room_action("create_room")

    @app.post("/api/rooms/join")
    def api_join_room():
        return room_action("join_room")

    @app.get("/api/rooms")
    def api_rooms():
        from signalling.client import SignallingError
        try:
            return jsonify(call(node.api_rooms))
        except SignallingError as e:
            return jsonify({"ok": False, "error": e.message, "code": e.code}), 503

    return app


class WebServer:
    """Runs the Flask app in a background thread; the node keeps running without browsers."""

    def __init__(self, node, host="127.0.0.1", port=8080):
        self.app = create_app(node)
        self.server = make_server(host, port, self.app, threaded=True)
        self.host, self.port = host, self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, name="web", daemon=True)

    def start(self):
        self.thread.start()
        log.info("web interface on http://%s:%s", self.host, self.port)
        return self

    def shutdown(self):
        self.server.shutdown()
        self.thread.join()
