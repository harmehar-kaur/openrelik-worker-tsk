# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-tsk.tasks.filesystem"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "openrelik-worker-TSK",
    "description": "Using TSK to parse and analyse Filesystem Files [MFT, J, BOOT, etc.]",
    # Configuration that will be rendered as a web for in the UI, and any data entered
    # by the user will be available to the task function when executing (task_config).
    "task_config": [ 
        { 
            "name": "artifact_types",
            "label": "Artifact Types",
            "description": "Select the file system artifacts to process.",
            "type": "checkbox",
            "options": [
                {"label": "MFT", "value": "mft"},
                {"label": "Journal", "value": "journal"},
                {"label": "Boot Log", "value": "boot_log"},
                {"label": "Logfile", "value": "logfile"},
                {"label": "Io3", "value": "io3"},
            ],
            "required": True,
        }
    ],
}


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run <REPLACE_WITH_COMMAND> on input files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []
    artifact_types = task_config.get("artifact_types", [])
    
    commands = {
        "mft": ["fls", "-r"],
        "journal": ["icat"],
        "boot_log": ["icat"],
        "logfile": ["icat"],
        "io3": ["icat"],
    }
    
    for input_file in input_files:
        file_name = input_file.get("display_name", "").lower()
        matched_type = next((t for t in artifact_types if t in file_name), None)
        
        if matched_type and matched_type in commands:
            output_file = create_output_file(
                output_path,
                display_name=input_file.get("display_name"),
                extension=".txt",
                data_type="filesystem_artifact",
            )
            
            command = commands[matched_type] + [input_file.get("path")]
            
            with open(output_file.path, "w") as fh:
                subprocess.Popen(command, stdout=fh)
            
            output_files.append(output_file.to_dict())
    
    if not output_files:
        raise RuntimeError("No valid file system artifacts processed.")
    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command="; ".join([" ".join(commands[t] + [f.get('path')]) for t, f in zip(artifact_types, output_files)]),
        meta={},
    )
