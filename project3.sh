rm ebin/*.beam

erlc -o ebin src/*.erl

source variables.env

#$ERLANG_BIN -pa "ebin" -eval "chord_supervisor:initiate_chord($1, $2)." -s init stop -noshell.