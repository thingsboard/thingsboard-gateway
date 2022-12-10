Flutter SDK & Dart Language
=======================================

Flutter  
--------------------------------------------------
Description 
    -Flutter is a free and open-source software development kit created by Google. 
    -Flutter is used for mobile application development and has the ability to craft “high-quality native interfaces” on both IOS and Android devices.  
 
Multiple Target OS Features 
   -Flutter uses the Dart programming language, which can be compiled to both Swift (native ios language) and Java (native Android language). This allows Flutter to be deployed on both ios and Android devices. 


Dart Programming Language  
---------------------------
Language Purpose 
    -Optimized for Creating User Interfaces 
    -Supports hot reloads so the developer can change their code and see the changes instantly while running their application  
    -Compile options 
        -ARM and x64 machine code for mobile, desktop, and backend 
        -Javascript for web  

Language Syntax 
    -Every application must have a main function, which acts as the entry point of the program 
    -Variables do not require explicit types 
        -The “var” keyword can be used to define any type of variable  
    -The three major control flow statements (if, for, and while) are all supported by Dart 
        -For loops can be written in the form of Python or C 
    -Functions are defined similar to C language  
        -Function return type and name must be given when defining a function 
        -Unlike C, function prototyping is not required  
    -Commenting follows the C commenting convention (//) 
    -Importing classes and APIs is accomplished through the “import” keyword 
    -Classes 
        -Global variables can be defined outside of functions at the top of the class 
        -Multiple constructors can be used to instantiate the class object  
        -Class functions are called using “class.function()” 
    -Enums are supported similar to the implementation in C# 
        -Defined using “enum enumName { item1, item2, item3 } 
    -Inheritance is supported similar to the implementation in Java 
        -Only single inheritance is supported 
        -@override is used in the same fashion an Java to override a function in a base class 
    -Interfaces are included 
        -There is no “implements” keyword, instead all classes are implicitly defined as an interface and can be implemented using the “implements” keyword  
    -Abstract classes are included and implemented similar to Java 
        -An abstract class contains function definitions to be implemented by a separate concrete class  
    -Exceptions are handled using try, on, and finally 
        -The “try” keyword is used similar to Java 
        -The “on” keyword catches the actual exception type 
        -The “finally” keyword runs its set of code at the end of the statement regardless of if an exception is caught  

 Important Libraries 
    -Dart:core 
        -Includes built-in types, collections, and other core functionality 
        -Library in automatically added to every Dart program  
    -Dart:async 
        -Support for concurrent programming 
    -Dart:math 
        -Includes mathematical constant and functions as well as a random number generator 
    -Dart:convert 
        -Encoders and decoders for converting between different data representations 
    -Dart:html 
        -DOM and other APIs for browser-based apps 
    -Dart:io 
        -Includes input and output capacities for Flutter apps, serves, and command-line scripts  

 Concurrency 
    -Dart supports concurrency by allowing developers to use futures, async, and await