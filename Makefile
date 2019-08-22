DOCKER_USER := icij
DOCKER_NAME := datashare-tarentula
VIRTUALENV := venv/
CURRENT_VERSION ?= `python setup.py --version`

clean:
		find . -name "*.pyc" -exec rm -rf {} \;

install: install_virtualenv install_pip

install_virtualenv:
		# Check if venv folder is already created and create it
		if [ ! -d venv ]; then virtualenv $(VIRTUALENV) --python=python3 --no-site-package --distribute; fi

install_pip:
		. $(VIRTUALENV)bin/activate; pip install --editable .

test:
		. $(VIRTUALENV)bin/activate; python setup.py test

minor:
		. $(VIRTUALENV)bin/activate; bumpversion --commit --tag --current-version ${CURRENT_VERSION} minor setup.py

major:
		. $(VIRTUALENV)bin/activate; bumpversion --commit --tag --current-version ${CURRENT_VERSION} major setup.py

patch:
		. $(VIRTUALENV)bin/activate; bumpversion --commit --tag --current-version ${CURRENT_VERSION} patch setup.py

docker-publish: docker-build docker-tag docker-push

docker-run:
		docker run -p 3000:3000 -it $(DOCKER_NAME)

docker-build:
		docker build -t $(DOCKER_NAME) .

docker-tag:
		docker tag $(DOCKER_NAME) $(DOCKER_USER)/$(DOCKER_NAME):${CURRENT_VERSION}
		docker tag $(DOCKER_NAME) $(DOCKER_USER)/$(DOCKER_NAME):latest

docker-push:
		docker push $(DOCKER_USER)/$(DOCKER_NAME):${CURRENT_VERSION}
		docker push $(DOCKER_USER)/$(DOCKER_NAME):latest
