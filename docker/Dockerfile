FROM condaforge/mambaforge:23.3.1-1

# Set timezone
ENV TZ=Europe/Paris
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Update and basic install
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata curl wget bzip2 build-essential git libgl1-mesa-glx xvfb libxrender1 && \
    apt-get clean autoclean && apt-get autoremove --yes && \
    rm -rf /var/lib/apt/lists/*

# Clone repos
WORKDIR /bluesky_blissdata

COPY . /bluesky_blissdata
    
# Setup environment
RUN conda config --append channels esrf-bcu && \
    conda config --append channels tango-controls

WORKDIR /bluesky_blissdata

RUN ["/bin/bash", "-c", ". /opt/conda/etc/profile.d/conda.sh && \
    . /opt/conda/etc/profile.d/mamba.sh && \
    mamba create -n bluesky_blissdata python=3.9 &&\
    conda config --env --append channels esrf-bcu &&\
    mamba activate  bluesky_blissdata && \
    mamba install 'blissdata=1.0.1' && \
    pip install -e . && \
    rm -rf /root/.cache/pip && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.pyc' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete"]

RUN ["/bin/bash", "-c", ". /opt/conda/etc/profile.d/conda.sh && \
    . /opt/conda/etc/profile.d/mamba.sh && \
    pip install --no-cache-dir pyzmq==22.3.0 supervisor multivisor[web]"]

COPY ./docker/supervisord.conf /etc/supervisor/supervisord.conf

ENTRYPOINT ["/bin/bash", "-c", ". /opt/conda/etc/profile.d/conda.sh && supervisord"]