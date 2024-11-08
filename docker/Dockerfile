FROM condaforge/mambaforge:23.3.1-1

# Set timezone
ENV TZ=Europe/Paris
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Update and basic install
RUN apt-get update && \
    apt-get install -y --no-install-recommends libhdf5-dev tzdata curl wget bzip2 build-essential git libgl1-mesa-glx xvfb libxrender1 && \
    apt-get clean autoclean && apt-get autoremove --yes && \
    rm -rf /var/lib/apt/lists/*

# Clone repos
WORKDIR /

RUN git clone https://gitlab.desy.de/fs-ec/bluesky_blissdata.git
    
# Setup environment
RUN conda config --append channels esrf-bcu && \
    conda config --append channels tango-controls

WORKDIR /bluesky_blissdata

RUN ["/bin/bash", "-c", ". /opt/conda/etc/profile.d/conda.sh && \
    . /opt/conda/etc/profile.d/mamba.sh && \
    mamba create -n bluesky_blissdata python=3.9 &&\
    conda config --env --append channels esrf-bcu &&\
    mamba activate  bluesky_blissdata && \
    mamba install h5py && \
    mamba install blissdata && \
    pip install -e . &&\
    # Bliss makefile will have used pip cache
    rm -rf /root/.cache/pip && \
    # https://jcristharif.com/conda-docker-tips.html
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.pyc' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete"]

RUN ["/bin/bash", "-c", ". /opt/conda/etc/profile.d/conda.sh && \
    . /opt/conda/etc/profile.d/mamba.sh && \
    pip install --no-cache-dir pyzmq==22.3.0 supervisor multivisor[web]"]

COPY ./docker/supervisord.conf /etc/supervisor/supervisord.conf

ENTRYPOINT ["/bin/bash", "-c", ". /opt/conda/etc/profile.d/conda.sh && supervisord"]