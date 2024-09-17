
SHELL := $(shell which zsh)

# Automate tool install process
install-tools: install-pyenv install-pipx install-pytools

install-pyenv:
	if ! command -v pyenv > /dev/null; then \
	brew update && brew upgrade; \
    brew install pyenv; \
    echo '# pyenv config' >> ~/.zshrc; \
    echo 'export PYENV_ROOT="$$HOME/.pyenv"' >> ~/.zshrc; \
    echo '[[ -d $$PYENV_ROOT/bin ]] && export PATH="$$PYENV_ROOT/bin:$$PATH"' >> ~/.zshrc; \
    echo 'eval "$$(pyenv init -)"' >> ~/.zshrc; \
    fi

install-pipx:
	if ! command -v pipx > /dev/null; then \
	brew update && brew upgrade; \
	brew install pipx; \
	pipx ensurepath; \
	fi

install-pytools:
	pipx install poetry
	pipx install jupyterlab

# Automate project setup process
project-setup:
	pyenv install --skip-existing $(PYTHON)
	pyenv local $(PYTHON)
	poetry init --no-interaction --python=$(PYTHON)
	poetry env use $(PYTHON)
	poetry install
	poetry add ipython ipykernel python-dotenv
	poetry run python -m ipykernel install --name $(shell basename $$(pwd))

# Automate project install process
project-install:
	$(eval PYTHON := $(shell cat .python-version))
	pyenv install --skip-existing $(PYTHON)
	pyenv local $(PYTHON)
	poetry install
	poetry env use $(PYTHON)
	poetry run python -m ipykernel install --name $(shell basename $$(pwd))
