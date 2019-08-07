FROM superphy/tox-base:d698931a52ddeef64dcf5918b32790bcef75af61

MAINTAINER Kevin Le <kevin.kent.le@gmail.com>

# TODO: switch to build stages, and rm prairiedog src after setup
WORKDIR /p
COPY . /p

# Update git submodules
RUN git submodule init && git submodule update --remote

# Setup PyPy3 virtualenv
ENV VIRTUAL_ENV=/opt/venv
RUN pypy3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:/p/gobin/go/bin:$PATH"

# Install deps
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# lemongraph
RUN apt-get update -y && apt-get install -y libffi-dev zlib1g-dev python-dev python-cffi libatlas-base-dev
RUN pip install --upgrade cffi
RUN cd lemongraph/ && python setup.py install

# setup.py
RUN python setup.py install

ENTRYPOINT ["snakemake", "-j"]
