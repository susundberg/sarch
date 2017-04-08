run_check:
	mypy sarch/*.py test/*.py
#	pylint3 --errors-only --output-format=parseable --disable=unsubscriptable-object,invalid-sequence-index  *.py sarch/*.py

run_test:
	python3 -m unittest


run_coverage:
	python3-coverage run --source sarch -m unittest
	python3-coverage html

pylint:
	pylint3 --errors-only --output-format=parseable *.py sarch/*.py

