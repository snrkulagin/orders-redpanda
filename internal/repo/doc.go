// Package repo is the only place in the project that knows about
// github.com/jackc/pgx. It exposes a Postgres-backed store on top of the
// orders database, kept behind plain Go methods so the rest of the project
// never has to import pgx directly.
package repo
