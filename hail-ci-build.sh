#!/bin/bash
set -ex

PROJECTS='hail batch ci site scorecard cloudtools upload'

for project in $PROJECTS; do
    if [[ -e $project/hail-ci-build.sh ]]; then
        CHANGED=$(python3 project-changed.py target/$TARGET_BRANCH $project)
        if [[ $CHANGED != no ]]; then
            (cd $project && /bin/bash hail-ci-build.sh)
        else
            echo "<p><span style=\"color:gray;font-weight:bold\">${project}: SKIPPED</span></p>" >> ${ARTIFACTS}/${project}.html
        fi
    fi
done
