# project/server/main/views.py

from http import HTTPStatus
from celery.result import AsyncResult
from flask import render_template, Blueprint, jsonify, request
from celery_once.tasks import AlreadyQueued

from project.server.tasks import damage_control

main_blueprint = Blueprint("main", __name__,)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")


@main_blueprint.route("/tasks", methods=["POST"])
def run_task():
    content = request.json
    name = content["name"]
    id = content["id"]

    try:
        res = damage_control.delay(id, name).get()
    except AlreadyQueued:
        return jsonify({"status": "same job running"}), HTTPStatus.ACCEPTED
    return jsonify({"status": "queued"}), HTTPStatus.CREATED


@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return jsonify(result), 200
