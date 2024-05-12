import tomllib
with open('pyproject.toml', 'rb') as f:
    dependencies = tomllib.load(f)['tool']['poetry']['dependencies']
cotainr_env_start = """
channels:
  - defaults
  - conda-forge
dependencies:
  - pip
  - jupyterlab
  - ipywidgets
  - mariadb-connector-c"""
with open("requirements.txt", "w") as rf, open("rocm-env.yml", "w") as rof, open("cuda-env.yml", "w") as cf:
    rof.write(cotainr_env_start)
    rof.write(f"""
  - python{dependencies['python'].replace('^', '>=')}
  - pip:
""")
    cf.write(cotainr_env_start)
    cf.write(f"""
  - python{dependencies['python'].replace('^', '>=')}
  - pip:
""")
    rof.write("    - --extra-index-url https://download.pytorch.org/whl/rocm5.6\n")
    # cf.write("    - --extra-index-url https://download.pytorch.org/whl/cu118\n")
    for dependency, version in dependencies.items():
        if dependency != "python":
            if type(version) is not str:
                if "git" in version:
                    dependency = f"git+{version['git']}"
                elif "url" in version:
                    dependency = f"{dependency} @ {version['url']}"
                else:
                    if "extras" in version:
                        dependency = f"{dependency}[{
                            ','.join(version['extras'])}]"
                    if "version" in version:
                        dependency = f"{dependency}{
                            version['version'].replace('^', '>=')}"
            else:
                dependency = f"{dependency}{version.replace('^', '>=')}"
            rf.write(f"{dependency}\n")
            rof.write(f"    - {dependency}\n")
            cf.write(f"    - {dependency}\n")
