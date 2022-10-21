rm ebin/*.beam

erlc -o ebin src/*.erl

source variables.env

$ERLANG_BIN -pa "ebin" -eval "initiate_chord:main($1, $2)." -s init stop -noshell.