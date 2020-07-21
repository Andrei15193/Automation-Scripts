module.exports = {
    getCommandLineOptions,
    contains
};

function getCommandLineOptions(args, multiValueOptions) {
    return args.reduce(
        function (result, arg) {
            if (arg.startsWith('-')) {
                this.currentOption = arg.substr(1);
                if (contains(multiValueOptions, arg))
                    result.named[this.currentOption] = result.named[this.currentOption] || [];
                else
                    result.named[this.currentOption] = result.named[this.currentOption] || true;
            }
            else {
                if (this.currentOption === null)
                    result.default.push(arg);
                else if (contains(multiValueOptions, this.currentOption))
                    result.named[this.currentOption].push(arg);
                else {
                    result.named[this.currentOption] = arg;
                    this.currentOption = null;
                }
            }
            return result;
        }.bind({ currentOption: null }),
        {
            named: {},
            default: []
        }
    );
}

function contains(array, item) {
    return array && array.indexOf(item) >= 0;
}