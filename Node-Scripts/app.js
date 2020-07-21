const path = require("path");
const fs = require("fs");

const args = process.argv.slice(2);
const commands = fs
    .readdirSync(__dirname)
    .filter(fileName => path.extname(fileName) == ".js")
    .map(fileName => path.basename(fileName, ".js"))
    .filter(fileName => fileName != path.basename(__filename, ".js"));

if (args.length == 0) {
    console.log("No command specified, available commands:");
    for (command of commands)
        console.log(command)
}
else if (commands.indexOf(args[0]) == -1) {
    console.log("Unknown command, available commands:");
    for (command of commands)
        console.log(command)
}
else {
    const command = require(path.join(__dirname, args[0]));
    const commandArgs = args.slice(1);
    if (commandArgs.length == 0 || commandArgs.indexOf("help") >= 0)
        command.writeHelp();
    else
        command.execute(commandArgs);
}
