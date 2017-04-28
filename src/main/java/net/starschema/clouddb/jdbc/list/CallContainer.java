/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc.list;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;

import net.starschema.clouddb.jdbc.Logger;

/**
 * Class that contains a functioncall of databasemetadata and stores the result
 * for later use
 *
 * @author Attila Horvath
 */
public class CallContainer {
    HashSet<ContainerResult> list;
    Logger logger = Logger.getLogger(CallContainer.class.getName());

    /**
     * Constructor which makes an empty list
     */
    public CallContainer() {

        this.list = new HashSet<ContainerResult>();
        logger.debug("Created Callcontainer");
    }

    /**
     * Adds a method to the list
     *
     * @param result     -
     * @param method     - method to add
     * @param parameters - the methods parameters
     */
    void AddCall(ResultSet result, Method method, List<Parameter> parameters) {
        this.list.add(new ContainerResult(result, method, parameters));
        logger.debug("Adding a Call to the Container");
    }

    //TODO javadoc, rename getresult to getResult
    ResultSet getresult(Method method, List<Parameter> parameters) {
        for (ContainerResult res : this.list) {
            boolean matches = true;
            if (res.getMethod().getName().equals(method.getName())) {
                int i = 0;
                if (res.getParameters().size() == parameters.size()) {
                    for (Parameter param : res.getParameters()) {
                        if (!param.equals(parameters.get(i))) {
                            matches = false;
                        }
                        i++;
                    }
                } else {
                    matches = false;
                }

            } else {
                matches = false;
            }
            if (matches) {
                return res.getResult();
            }
        }
        // Didn't find any matches
        return null;
    }

}

/**
 * A class to be used with the CallContainer
 */
class ContainerResult {
    ResultSet result;
    Method method;
    List<Parameter> parameters;

    /**
     * Constructor for ContainerResult
     */
    public ContainerResult(ResultSet result, Method method,
                           List<Parameter> parameters) {
        this.result = result;
        this.method = method;
        this.parameters = parameters;
    }

    /** Getter for the method */
    public Method getMethod() {
        return this.method;
    }

    /** Getter for the parameters */
    public List<Parameter> getParameters() {
        return this.parameters;
    }

    /** Getter for the result */
    public ResultSet getResult() {
        return this.result;
    }

}

/**
 * A class to store the parameter as an Object
 * which can be String or String[]
 *
 * @author Attila Horvath
 */
class Parameter {
    Object parameter;

    /**
     * Constructor for Parameter,
     * it's {@link #equals(Object)} function handles String[] and String too
     *
     * @param param - String or String[] or an Object
     */
    public Parameter(Object param) {
        this.parameter = param;
    }

    /**
     * A comparer to compare 2 object, mainly
     * <li> String[]
     * <li> String
     * <li> Objects
     */
    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(this.getClass())) {
            if (this.parameter.getClass().equals(String[].class)) {
                String[] list1 = String[].class.cast(this.parameter);
                String[] list2 = null;
                try {
                    list2 = String[].class
                            .cast(this.getClass().cast(obj).parameter);
                } catch (ClassCastException e) {
                    return false;
                }
                boolean matches = true;
                if (list2.length != list1.length) {
                    return false;
                }
                int i = 0;
                for (String s : list1) {
                    if (!s.equals(list2[i])) {
                        matches = false;
                    }

                    i++;
                }
                return matches;
            } else {
                return this.getClass().cast(obj).parameter.toString().equals(
                        this.parameter.toString());
            }
        } else {
            return super.equals(obj);
        }
    }
}
