export GLIBCXX_FORCE_NEW=1
set |grep CXX
valgrind --leak-check=yes --track-origins=yes --tool=memcheck --suppressions=upscaledb.supp "$@"
