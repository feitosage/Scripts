# Quando for instalar o python pela primeira vez
library(reticulate)

version = '3.12.6'
install_python(version = version)
virtualenv_create('my-python', python_version = version)

# Rodar todas as vezes antes de utilizar python num script R ou no Console

use_virtualenv("my-python", required = TRUE)

# Para instalar um pacote é necessário rodar o comando abaixo
virtualenv_install(envname = "my-python","numpy", ignore_installed = FALSE,pip_options = character())
virtualenv_install(envname = "my-python","pandas", ignore_installed = FALSE,pip_options = character())
virtualenv_install(envname = "my-python","openpyxl", ignore_installed = FALSE,pip_options = character())
virtualenv_install(envname = "my-python","matplotlib", ignore_installed = FALSE,pip_options = character())

