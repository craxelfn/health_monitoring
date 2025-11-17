package com.stream.processor.validation;


import lombok.Value;

import java.util.ArrayList;
import java.util.List;

@Value
public class ValidationResult {
    boolean valid ;
    List<String> errors ;

    public static ValidationResult valid(){
        return new ValidationResult(true , new ArrayList<>()) ;
    }

    public static ValidationResult invalid(String error){
        List<String> errors = new ArrayList<>();
        errors.add(error);
        return new ValidationResult(false, errors);
    }

    public ValidationResult combine(ValidationResult other){
        if(this.valid && other.valid){
            return valid() ;
        }
        List<String> allErrors = new ArrayList<>(this.errors);
        allErrors.addAll(other.errors);
        return new ValidationResult(false, allErrors);
    }
}
