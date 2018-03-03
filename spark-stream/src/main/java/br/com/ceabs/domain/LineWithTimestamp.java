package br.com.ceabs.domain;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;


/**
 * User-defined data type representing the raw lines with timestamps.
 */

public class LineWithTimestamp implements Serializable {

    @Getter
    @Setter
    private String line;

    @Getter @Setter
    private Timestamp timestamp;
}

