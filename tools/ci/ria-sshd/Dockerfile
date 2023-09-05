FROM debian:stable-slim

# install the OpenSSH with the least fuzz
RUN apt-get update -qq -y --allow-releaseinfo-change
RUN apt-get install -q --no-install-recommends -y openssh-server
RUN apt-get clean

# setup 'riauser' account, with the provided SSH key authorized for
# login, and a 'riauserpwd' default password
RUN useradd -m -d /home/riauser -s /bin/bash -u 1000 riauser
RUN echo 'riauser:riauserpwd' | chpasswd
COPY id_rsa.pub /home/riauser/.ssh/authorized_keys
RUN chown -R riauser:riauser /home/riauser/.ssh
RUN chmod 600 /home/riauser/.ssh/authorized_keys

# configure SSHD for startup
RUN service ssh start
EXPOSE 22/tcp

# run the SSHD process in the foreground
CMD ["/usr/sbin/sshd","-D"]
