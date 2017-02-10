# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|

  # the base box is ubuntu 16.10
  config.vm.box = "bento/ubuntu-16.10"

  # check update
  config.vm.box_check_update = true

  # forward guest 80 to host 8080
  # config.vm.network "forwarded_port", guest: 80, host: 8080

  config.vm.provider "virtualbox" do |vb|
     # Don't show the GUI unless you have some bug
     vb.gui = false
     # Customize the amount of memory on the VM:
     vb.memory = "1024"
  end

  config.vm.provision "shell", path: "setup_ubuntu.sh"

end