Downloading ThingsBoard Mobile App
=======================================

Cloning the repository
--------------------------------------------------
#. Open a web browser and go to the following link: 
    -https://github.com/lbrogan1/VarIoT_ThingsBoard_Mobile_App
#. Copy the github repository link, you can copy the link below:
    -https://github.com/lbrogan1/VarIoT_ThingsBoard_Mobile_App.git
#. Open Git Bash (If you don’t have Git Bash, go to section labeled ‘Installing Git Bash’)  
    -Using the ‘cd’ command, navigate to the directory you wish to clone the source code to 
    -Once in the directory, enter the command ‘git clone (link copied from above)’ 

Opening App in Visual Studio (VS) Code 
---------------------------
#. Open VS Code application  
#. In the top left corner 
    -Select ‘File’ 
    -Select ‘Open Folder..’ 
#. Navigate to the directory where you cloned the repository
#. Enter the directory  
    -Select ‘flutter_thingsboard_app’ 
    -Select ‘Select Folder’ 

Installing Flutter SDK
---------------------
#. Open a web browser and go to the following link 
    -https://docs.flutter.dev/get-started/install 
#. Select your operating system  
#. Click the button ‘flutter_windows_3.3.8-stable.zip’ 
    -Windows will be your current operating system 
#. Open the downloaded zip file 
    -Select ‘Extract all’ 
    -Select the destination folder you wish to extract the Flutter SDK files to  
        -This may take a bit – the files are 2.3 GB in size  
#. Add project repository onto the current OS path
    -On Windows, the easiest way is to manually alter the settings.
        -Go to the start bar and write 'env', click on 'Edit environment variables for your account'.
        -Don't go to the similarly labeled 'Edit the system environment variables'
        -On the Path variable, add in the value of the path to 'flutter/bin', .i.e C:\Users\$USER\flutter\bin
    -On macOS, you will have to use the shell within your flutterSDK project.
        -Type '$echo shell' to determine whether you're using Bash, Z shell, or wome other shell
        -Open or create the rc file for your shell. 
            -If you’re using Bash, edit "$HOME/.bash_profile" or "$HOME/.bashrc 
            -If you’re using Z shell, edit "$HOME/.zshrc"" 
            -If you’re using a different shell, the file path and filename will be different on your machine.
        -In the rc file, add the following line: export PATH="$PATH:[PATH_OF_FLUTTER_GIT_DIRECTORY]/bin"
        -Replace [PATH_OF_FLUTTER_GIT_DIRECTORY] with the actual path to your clone of the FlutterSDK
        -Run source $HOME/.<rc file> to refresh the current window, or open a new terminal window to automatically source the file.
        -Verify that the flutter/bin directory is now in your PATH by running: 'echo $PATH'
#. Open terminal in repository and run "flutter doctor" to determine any missing dependencies 
    -Requires Chrome, Visual Studio, and Android Studio downloaded 
    -Download Android studio through: https://developer.android.com/studios

Installing Dart extension
-----------------------------
#. In the top left corner 
    -Select ‘Run’ 
    -Select ‘Start Debugging’ 
#. A message will appear 
    -Select ‘Find Dart extension’ 
#. On the left hand side 
    -Select ‘Install’ under the ‘Dart’ extension  
#. A message will appear in the bottom right saying that Flutter SDK could not be found 
    -Select ‘Locate SDK’ 
#. Navigate to where you extracted the flutter files 
#. Select the ‘flutter’ file and select ‘Set Flutter SDK Folder’

Setting up Web Platform
-------------
#. In the top left 
    -Select ‘Run’ 
    -Select ‘Start Debugging’ 
#. Select “Enable web for this project’ 
#. In the bottom right, a message will appear asking if you want to add the web platform to this project 
    -Select ‘Yes’ 

Running the Application
-----------------------
#. In the top left 
    -Select ‘Run’ 
    -Select ‘Start Debugging’ 
#. A menu will open in the top middle of the screen 
    -Select the browser you wish to use to run the app 
#. It may take a couple minutes to open the first time you run it 
 
Congratulations, the app should be up and running! 


Installing Git Bash 
-------------
#. Open a web browser and go to the following link 
    -https://git-scm.com/downloads 
#. Select your operating system  
#. Under the ‘Standalone Installer’ section 
    -Depending on your computer, select the 32-bit or 64-bit download link 
#. Open the downloaded executable file 
#. Navigate through the setup wizard 
#. Wait for executables to be installed  

