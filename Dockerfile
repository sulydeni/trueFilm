FROM amazonlinux:2

# Linux Dependencies
RUN yum install postgresql java-1.8.0-openjdk python3 python3-pip -y

# Python Packages
RUN python3 -m pip install pypandoc jupyter pandas sqlalchemy --user
RUN python3 -m pip install pyspark --user
RUN python3 -m pip install psycopg2-binary --user


# Expose Jupyter Notebook
EXPOSE 8888

# Command
RUN mkdir -p /opt/code
CMD ["python3","-m","notebook","--port=8888","--no-browser","--ip=0.0.0.0","--notebook-dir=/opt/code","--allow-root"]
