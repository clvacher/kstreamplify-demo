package com.bdxio.stream.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ProcessResult {

    private String value;

    private Exception exception;


}
