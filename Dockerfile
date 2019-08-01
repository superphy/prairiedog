FROM superphy/tox-base:d698931a52ddeef64dcf5918b32790bcef75af61

MAINTAINER Kevin Le <kevin.kent.le@gmail.com>

WORKDIR /pdg
COPY . /pdg

# Setup PyPy3 virtualenv
ENV VIRTUAL_ENV=/opt/venv
RUN pypy3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install deps
RUN pip install -r requirements.txt

ENTRYPOINT ["snakemake"]
