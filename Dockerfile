FROM superphy/tox-base:af2794de54ad6edf097552d06d69be541f3bd419

MAINTAINER Kevin Le <kevin.kent.le@gmail.com>

# TODO: switch to build stages, and rm prairiedog src after setup
WORKDIR /p
COPY . /p

# Update git submodules
RUN git submodule init && git submodule update --remote

# Setup Python3 virtualenv
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
#ENV PATH="$VIRTUAL_ENV/bin:/p/gobin/go/bin:$PATH"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dgraph
RUN curl https://get.dgraph.io -sSf | bash

# Install deps
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# lemongraph
RUN apt-get update -y && apt-get install -y libffi-dev zlib1g-dev python-dev python-cffi libatlas-base-dev
RUN pip install --upgrade cffi
RUN cd lemongraph/ && python setup.py install

# setup.py
RUN python setup.py install

ENTRYPOINT ["prairiedog"]
