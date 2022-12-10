ThingsBoard Mobile App and Aware Framework Integration
=======================================

ThingsBoard Mobile App and Aware Framework Integration 
--------------------------------------------------
Goal 
    -To create a single mobile application that acts as a dashboard for our VarIoT gateway and has the ability to read and display data from internal phone sensors  


Plan to achieve goal 
	-Use the ThingsBoard Mobile App source code as a base and add the necessary files from Aware to achieve desired functionality 


Steps Taken Thus Far 
    -Within Aware… 
        -Located all of the files that are responsible for data collecting and data displaying  
	        -Moved these files into our ThingsBoard Mobile App repository  
                -These files aren’t actually abstracted (connected) to the source code yet 
                -They are just sitting in a folder in the main directory  
        -Located the different APIs that are used to collect and display internal sensor data 
            -IOS API: CoreMotion API 
            -Android API: Android Sensor Framework 
            -Front-end display API: GetUiKit 
                -Beginning the process of finding and reading documentation for these APIs. I will create documentation on all as I learn. 

    -Within ThingsBoard… 
        -Created the GitHub repository for our instance of the application 
        -The application uses a back-end API to connect the application to our VarIoT server 
            -Our API: http://variot.ece.drexel.edu:8080/  
                -Have used this API to run the application, but only client-side components appear successfully. App is still not connected to server. 

    -Other tasks… 
        -Created a script that will parse the data from the Aware database and put it in data structure that is able to be uploaded to ThingsBoard as a device 
            -This file will eventually be used as a link between the Aware files and the ThingsBoard files 
 

Next Steps 
    -Within Aware… 
        -Learn how to use the APIs that were found in the Aware files 
            -Create documentation for them 
            -Figure out how to use these APIs within ThingsBoard 
                -What functions will we need to take from Aware? 
                -What functions will we have to rewrite? 

    -Within ThingsBoard… 
        -There is an issue with the difference in coding languages between the Aware IOS files, Aware Android files, and ThingsBoard Mobile App files 
            -Aware IOS: Matlab 
            -Aware Android: Java 
            -ThingsBoard: Dart  
                -Need to determine how to convert all of these files to the same languages so they can interact with each other  
        -Determine an access point for the Aware files to interact with ThingsBoard files 
            -How often will the internal sensors be updated? 
                -Is it going to be live updates? 
                -Update after a certain amount of time? 

    -Parser File… 
        -Locate where in the ThingsBoard file structure the file parser will go 
        -How will the parser be used to automate the data transfer process? 

    -Flutter SDK… 
        -Need to read documentation on Flutter and how it is used/deployed 
            -I will create a documentation to follow 

GitHub Repository 
---------------------------
#. Repository link: https://github.com/lbrogan1/VarIoT_ThingsBoard_Mobile_Application.git 
    -This will be changed in the coming week to the DWSL repository 
        -I have to figure out exactly where to put it 
