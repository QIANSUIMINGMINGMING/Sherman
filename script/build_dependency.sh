mkdir ~/tools
cd ~/tools

# Install cityhash
git clone https://github.com/google/cityhash.git
cd cityhash
./configure
make all check CXXFLAGS="-g -O3"
sudo make install
cd ..

# Install memcached
sudo apt-get install libmemcached-dev
sudo apt-get install memcached

# Install ibverbs
sudo apt-get install libibverbs-dev

# Install boost


