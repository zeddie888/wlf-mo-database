"use strict";
const express = require("express");
const multer = require("multer");
const sqlite = require("sqlite");
const sqlite3 = require("sqlite3");

const PORT_NUMBER = 8000;

const app = express();
app.use(multer().none());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// app.get("/moDB/");

/**
 * Establishes a database connection to the database and returns the database object.
 * Any errors that occur are caught in the function that calls this function.
 * @returns {Object} - The database object for the connection.
 */
async function getDBConnection() {
  const db = await sqlite.open({
    filename: "bookstore.db",
    driver: sqlite3.Database,
  });
  return db;
}

app.use(express.static("public"));
const PORT = process.env.PORT || PORT_NUMBER;
app.listen(PORT);
