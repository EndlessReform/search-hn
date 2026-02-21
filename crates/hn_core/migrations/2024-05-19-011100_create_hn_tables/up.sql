-- Your SQL goes here
CREATE TABLE "kids"(
	"item" INT8 NOT NULL,
	"kid" INT8 NOT NULL,
	"display_order" INT8,
	PRIMARY KEY("item", "kid")
);

CREATE TABLE "items"(
	"id" INT8 NOT NULL PRIMARY KEY,
	"deleted" BOOL,
	"type" TEXT,
	"by" TEXT,
	"time" INT8,
	"text" TEXT,
	"dead" BOOL,
	"parent" INT8,
	"poll" INT8,
	"url" TEXT,
	"score" INT8,
	"title" TEXT,
	"parts" TEXT,
	"descendants" INT8
);

CREATE TABLE "users"(
	"id" TEXT NOT NULL PRIMARY KEY,
	"created" INT8,
	"karma" INT8,
	"about" TEXT,
	"submitted" TEXT
);

